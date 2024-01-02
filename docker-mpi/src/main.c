#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <sys/stat.h>
#include <mpi.h>
#include <ctype.h>

#define MASTER_RANK 0
#define INPUT_DIR "./InputFile"
#define MAX_WORD_LENGTH 50

typedef struct OccurrenceNode
{
    int count;
    char word[MAX_WORD_LENGTH];
    struct OccurrenceNode *next;
} OccurrenceNode;

// Struttura Hashtable
typedef struct
{
    unsigned int size;
    OccurrenceNode **table;
} Hashtable;

typedef struct HistogramNode
{
    char word[MAX_WORD_LENGTH];
    int count;
} HistogramNode;

// Dichiarazione della hashtable globale e della hashtable globale nel master
Hashtable localHashtable, masterHashtable;
OccurrenceNode *local_occurrences = NULL;

int local_total_occurrences = 0;

// strut per occorrenze locali
typedef struct LocalOccurrenceNode
{
    char word[MAX_WORD_LENGTH];
    int count;
    struct LocalOccurrenceNode *next;
} LocalOccurrenceNode;

LocalOccurrenceNode *local_occurrences_head = NULL;

int totalParole = 0;

// dichiarazione della struttura MPI_OCCURRENCE_NODE
MPI_Datatype MPI_OCCURRENCE_NODE;

// per inizializzare la hashtable
void initializeHashtable(unsigned int size)
{
    localHashtable.size = size;

    // Alloca dinamicamente la memoria per la tabella hash
    localHashtable.table = (OccurrenceNode **)malloc(size * sizeof(OccurrenceNode *));

    // Inizializza tutti gli elementi della tabella hash a NULL
    for (unsigned int i = 0; i < size; ++i)
    {
        localHashtable.table[i] = NULL;
    }
}

// per deallocare la memoria della hashtable
void freeHashtable()
{
    // Dealloca le liste di trabocco e la tabella hash stessa
    for (unsigned int i = 0; i < localHashtable.size; ++i)
    {
        OccurrenceNode *current = localHashtable.table[i];
        while (current != NULL)
        {
            OccurrenceNode *temp = current;
            current = current->next;
            free(temp);
        }
    }
    free(localHashtable.table);

    if (local_occurrences != NULL)
    {
        free(local_occurrences);
        local_occurrences = NULL; // Imposta il puntatore a NULL dopo la liberazione
    }
}

void removePunctuation(char *word)
{
    // per rimuovere la punteggiatura e anche spazi, dalla parola e convertire le lettere a minuscolo
    int i, j = 0;
    for (i = 0; word[i]; ++i)
    {
        if (isalpha(word[i]) || isspace(word[i]))
        {
            word[j++] = tolower(word[i]);
        }
    }
    word[j] = '\0';
}

// Funzione che conta le occorrenze
void updateWordCount(const char *word, int rank)
{
    // Rimuovi la punteggiatura dalla parola
    char cleanedWord[MAX_WORD_LENGTH];
    // copia il contenuto di word in cleanedWord
    strncpy(cleanedWord, word, MAX_WORD_LENGTH - 1);
    cleanedWord[MAX_WORD_LENGTH - 1] = '\0'; // Assicura che la stringa sia terminata correttamente
    removePunctuation(cleanedWord);

    // Cerca la parola nella lista di occorrenze locali

    // Inizializza un puntatore 'current' al primo nodo della lista locale di occorrenze
    LocalOccurrenceNode *current = local_occurrences_head;
    while (current != NULL)
    {
        // Verifica se la parola corrente nella lista è uguale alla parola pulita
        if (strcmp(current->word, cleanedWord) == 0)
        {
            // se esiste nella lista locale, incrementa il conteggio
            current->count++;
            //printf("Processo %d: Word %s found. Count updated to %d\n", rank, cleanedWord, current->count);
            return;
        }
        // Passa al nodo successivo nella lista
        current = current->next;
    }

    // La parola non esiste nella lista locale, aggiungila
    LocalOccurrenceNode *newNode = (LocalOccurrenceNode *)malloc(sizeof(LocalOccurrenceNode));
    if (newNode == NULL)
    {
        // Gestisci l'errore di allocazione della memoria
        printf("Processo %d: Errore - Allocazione di memoria fallita.\n", rank);
        return;
    }

    // Inizializza il nuovo nodo con un conteggio di 1 (prima occorrenza)
    newNode->count = 1;

    // Copia la parola pulita nel campo word del nuovo nodo
    strncpy(newNode->word, cleanedWord, MAX_WORD_LENGTH - 1);
    newNode->word[MAX_WORD_LENGTH - 1] = '\0'; // Assicura che la stringa sia terminata correttamente

    // Aggiorna il puntatore al prossimo nodo nella lista con il puntatore alla testa attuale
    newNode->next = local_occurrences_head;

    // Aggiorna il puntatore alla testa della lista locale con il nuovo nodo
    local_occurrences_head = newNode;

    // Stampa il processo, la parola aggiunta e il conteggio impostato a 1
    //printf("Processo %d: Word %s added. Count set to 1\n", rank, cleanedWord);
}

long get_total_file_size(DIR *dir, const char *input_dir)
{
    // Inizializzo la variabile 'total_size' a zero, che conterrà la dimensione totale dei file nella directory.
    long total_size = 0;

    // Dichiaro una struttura 'entry' per rappresentare una voce nella directory e una struttura 'info' per ottenere informazioni sul file.
    struct dirent *entry;
    struct stat info;

    // Ciclo attraverso tutte le voci nella directory.
    while ((entry = readdir(dir)) != NULL)
    {
        // Ignoro le voci che rappresentano le directory "." e "..".
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
        {
            continue;
        }

        // Costruisco il percorso completo del file utilizzando la directory di input e il nome del file.
        char filepath[1024];
        snprintf(filepath, sizeof(filepath), "%s/%s", input_dir, entry->d_name);

        // Ottengo le informazioni sul file utilizzando 'stat' e aggiungo la dimensione del file a 'total_size'.
        if (stat(filepath, &info) == 0)
        {
            total_size += info.st_size;
        }
    }

    // Riavvolgo la directory per consentire una successiva lettura.
    rewinddir(dir);

    // Restituisco la dimensione totale dei file nella directory.
    return total_size;
}

void sendLocalOccurrences(int rank)
{
    LocalOccurrenceNode *current = local_occurrences_head;
    int local_occurrences_count = 0;

    // Conta il numero di occorrenze locali
    while (current != NULL)
    {
        local_occurrences_count++;
        current = current->next;
    }

    // Invia il numero di occorrenze locali al processo MASTER
    MPI_Send(&local_occurrences_count, 1, MPI_INT, MASTER_RANK, 0, MPI_COMM_WORLD);

    // Invia le occorrenze locali al processo MASTER
    current = local_occurrences_head;
    while (current != NULL)
    {
        // Pulisce la parola prima di inviarla
        removePunctuation(current->word);

        MPI_Send(current, 1, MPI_OCCURRENCE_NODE, MASTER_RANK, 0, MPI_COMM_WORLD);
        current = current->next;
    }
}

void initializeMasterHashtable(int size)
{
    masterHashtable.size = size;
    // Alloca dinamicamente la memoria per la tabella hash del master
    masterHashtable.table = (OccurrenceNode **)malloc(size * sizeof(OccurrenceNode *));

    // Inizializza tutti gli elementi della tabella hash a NULL
    for (int i = 0; i < size; ++i)
    {
        masterHashtable.table[i] = NULL;
    }
}

int hashFunction(const char *word, int tableSize)
{
    // l'indice risultante verrà utilizzato per posizionare la parola nella tabella hash.
    unsigned int hash = 5381;

    while (*word)
    {
        hash = ((hash << 5) + hash) + *word++; // faccio shift, somma e XOR per assegnare un valore ad
    }
    return (int)(hash % tableSize);
}

void updateGlobalHistogram(LocalOccurrenceNode *localHistogram, int localHistogramSize)
{
// Inizializzo un puntatore alla testa della lista di occorrenze locali.
LocalOccurrenceNode *currentNode = localHistogram;

// Scorro la lista di occorrenze locali.
while (currentNode != NULL)
{
    // Creo una copia della parola corrente, la pulisco da eventuali segni di punteggiatura e la converto in minuscolo.
    char cleanedWord[MAX_WORD_LENGTH];
    strncpy(cleanedWord, currentNode->word, MAX_WORD_LENGTH - 1);
    cleanedWord[MAX_WORD_LENGTH - 1] = '\0';
    removePunctuation(cleanedWord);

    for (int i = 0; cleanedWord[i]; i++)
    {
        cleanedWord[i] = tolower(cleanedWord[i]);
    }

    // Calcolo l'indice hash per la parola pulita utilizzando la funzione hashFunction.
    unsigned int hashIndex = hashFunction(cleanedWord, masterHashtable.size);

    // Stampo a schermo informazioni di debug sull'aggiornamento.
    //printf("Aggiornamento istogramma globale - Parola: %s, Indice Hash: %u\n", cleanedWord, hashIndex);

    // Accedo alla tabella hash globale all'indice calcolato.
    OccurrenceNode *current = masterHashtable.table[hashIndex];
    int wordFound = 0; // Flag che indica se la parola è stata trovata.

    // Scorro la lista di occorrenze globali corrispondenti all'indice hash.
    while (current != NULL)
    {
        // Se la parola è già presente nell'istogramma globale, aggiorno il conteggio.
        if (strcmp(current->word, cleanedWord) == 0)
        {
            current->count += currentNode->count;
            //printf("Parola esistente. Aggiornato conteggio a %d\n", current->count);
            wordFound = 1;
            break; // Termina il loop se la parola è già presente.
        }
        current = current->next;
    }

    // Se la parola non è stata trovata nell'istogramma globale, la aggiungo.
    if (!wordFound)
    {
        OccurrenceNode *newNode = (OccurrenceNode *)malloc(sizeof(OccurrenceNode));
        if (newNode == NULL)
        {
            // Gestisco l'errore di allocazione di memoria.
            printf("Processo MASTER: Errore - Allocazione di memoria fallita.\n");
            return;
        }

        // Copio il conteggio e la parola pulita nel nuovo nodo.
        newNode->count = currentNode->count;
        strncpy(newNode->word, cleanedWord, MAX_WORD_LENGTH - 1);
        newNode->word[MAX_WORD_LENGTH - 1] = '\0';

        // Collego il nuovo nodo alla lista di occorrenze globali all'indice hash.
        newNode->next = masterHashtable.table[hashIndex];
        masterHashtable.table[hashIndex] = newNode;

        //printf("Nuova parola aggiunta. Conteggio: %d\n", newNode->count);
    }

    // Passo alla successiva occorrenza locale.
    currentNode = currentNode->next;
}
}

/* 
// funzione di debug per verificare se le occorrenze locali erano corrette prima dell' invio.
void printLocalOccurrences(LocalOccurrenceNode *head)
{
    LocalOccurrenceNode *current = head;

    while (current != NULL)
    {
        printf("Parola: %s, Count: %d\n", current->word, current->count);
        current = current->next;
    }
}*/

void receiveAndMergeHistograms(int num_processes)
{
    MPI_Status status;

    // Ciclo fino a quando non si sono ricevuti tutti gli istogrammi
    for (int i = 1; i < num_processes; ++i)
    {
        int source_rank;
        MPI_Probe(MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
        source_rank = status.MPI_SOURCE;

        // Ottieni il numero di LocalOccurrenceNode nel messaggio in arrivo
        int receivedCount;
        MPI_Get_count(&status, MPI_INT, &receivedCount);

        // Ricevi il numero di occorrenze locali dal processo SLAVE
        MPI_Recv(&receivedCount, 1, MPI_INT, source_rank, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        totalParole = totalParole + receivedCount;

        // Ricevi le occorrenze locali dal processo SLAVE
        LocalOccurrenceNode *receivedBuffer = NULL;
        LocalOccurrenceNode *lastNode = NULL;

        for (int j = 0; j < receivedCount; ++j)
        {
            LocalOccurrenceNode *newNode = (LocalOccurrenceNode *)malloc(sizeof(LocalOccurrenceNode));
            newNode->count = 0; // Inizializzazione del conteggio a zero
            if (newNode == NULL)
            {
                // Gestisci l'errore di allocazione della memoria
                printf("Processo MASTER: Errore - Allocazione di memoria fallita.\n");
                MPI_Abort(MPI_COMM_WORLD, 1);
            }

            // Ricevi l'elemento corrente
            MPI_Recv(newNode, 1, MPI_OCCURRENCE_NODE, source_rank, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            // Pulisci la parola dopo averla ricevuta
            removePunctuation(newNode->word);

            // Aggiungi il nuovo nodo alla lista concatenata
            newNode->next = NULL;
            if (receivedBuffer == NULL)
            {
                receivedBuffer = newNode;
                lastNode = newNode;
            }
            else
            {
                lastNode->next = newNode;
                lastNode = newNode;
            }
        }

        // Aggiungi stampe di debug per verificare le occorrenze ricevute
        //printf("Processo MASTER: Ricevuto %d occorrenze locali dal processo %d.\n", receivedCount, source_rank);
        //printLocalOccurrences(receivedBuffer);     // Aggiungi una funzione di stampa per le occorrenze locali
        totalParole = totalParole + receivedCount; // Aggiorna il conteggio totale delle parole

        // Aggiorna l'istogramma globale con le occorrenze ricevute
        updateGlobalHistogram(receivedBuffer, receivedCount);
    }
}

// Funzione di confronto per l'ordinamento in base al conteggio delle occorrenze (decrescente)
int compareOccurrences(const void *a, const void *b)
{
    const OccurrenceNode *occurrenceA = (const OccurrenceNode *)a;
    const OccurrenceNode *occurrenceB = (const OccurrenceNode *)b;

    // Ordina in ordine decrescente
    return (occurrenceB->count - occurrenceA->count);
}

// Funzione per ordinare la lista di occorrenze
void sortOccurrencesList(OccurrenceNode **list)
{
    // Utilizza qsort per ordinare la lista in base al conteggio delle occorrenze
    qsort(*list, (*list)->count, sizeof(OccurrenceNode), compareOccurrences);
}

// Funzione per ordinare la hashtable globale
void sortGlobalHashtable(Hashtable *hashTable)
{
    for (unsigned int i = 0; i < hashTable->size; ++i)
    {
        // Ordina la lista di occorrenze per ogni elemento nella hashtable
        sortOccurrencesList(&(hashTable->table[i]));
    }
}

void writeSortedArrayToCSV(const char *filename, OccurrenceNode *occurrencesArray, int totalOccurrences)
{
    FILE *csvFile = fopen(filename, "w");
    if (csvFile == NULL)
    {
        perror("Errore nell'apertura del file CSV");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    // Intestazione del file CSV
    fprintf(csvFile, "Il numero finale delle occorrenze di ogni Word è il seguente (ordinato per conteggio decrescente).\n");

    // Scrittura dei dati dall'array ordinato
    for (int i = 0; i < totalOccurrences; ++i)
    {
        // Pulisci la parola prima di scriverla
        char cleanedWord[MAX_WORD_LENGTH];
        strcpy(cleanedWord, occurrencesArray[i].word);
        removePunctuation(cleanedWord);

        // Scrivi nel file CSV
        fprintf(csvFile, "Parola: %s - Occorrenze: %d\n", cleanedWord, occurrencesArray[i].count);
    }

    fclose(csvFile);
}


int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);

    int rank, num_processes;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &num_processes);
     double start_time, end_time;
    // Inizio del cronometro
   // start_time = MPI_Wtime();
    // Dichiarazioni e inizializzazioni necessarie
    MPI_Request request_send_size[num_processes - 1];
    MPI_Request request_send_buffer[num_processes - 1];
    MPI_Request request_receive_size[num_processes - 1];
    MPI_Request request_receive_buffer[num_processes - 1];
    MPI_Status status;

    // Dichiarazione delle variabili per MPI
    int blockcounts[2] = {1, MAX_WORD_LENGTH};
    MPI_Aint offsets[2] = {offsetof(OccurrenceNode, count), offsetof(OccurrenceNode, word)};
    MPI_Datatype types[2] = {MPI_INT, MPI_CHAR};

    MPI_Type_create_struct(2, blockcounts, offsets, types, &MPI_OCCURRENCE_NODE);
    MPI_Type_commit(&MPI_OCCURRENCE_NODE);

    // Sposta la dichiarazione fuori dai rami condizionali
    DIR *dir = opendir(INPUT_DIR);
    if (!dir)
    {
        perror("Errore nell'apertura della directory");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    long total_size = get_total_file_size(dir, INPUT_DIR);
    // Invia la dimensione totale del file a tutti i processi
    MPI_Bcast(&total_size, 1, MPI_LONG, MASTER_RANK, MPI_COMM_WORLD);
    // Array per contenere tutte le OccurrenceNode ricevute dai processi SLAVE

    if (rank == MASTER_RANK)
    {
        OccurrenceNode *allOccurrencesArray = NULL;
        OccurrenceNode *finalHistogram = NULL;

        struct dirent *entry;
        int file_count = 0;

        // Conta il numero di file nella directory
        while ((entry = readdir(dir)) != NULL)
        {
            if (entry->d_type == DT_REG) // Verifica se è un file regolare
            {
                file_count++;
            }
        }

        rewinddir(dir);

        // Allocazione di un buffer per contenere tutti i dati da inviare
        char *all_data = (char *)malloc(total_size * sizeof(char));

        // Leggi tutti i dati dai file nella directory nel buffer all_data
        long current_offset = 0;
        while ((entry = readdir(dir)) != NULL)
        {
            if (entry->d_type == DT_REG) // Verifica se è un file regolare
            {
                char filepath[1024];
                snprintf(filepath, sizeof(filepath), "%s/%s", INPUT_DIR, entry->d_name);

                FILE *file = fopen(filepath, "r");
                if (file != NULL)
                {
                    fseek(file, 0, SEEK_END);
                    long file_size = ftell(file);
                    fseek(file, 0, SEEK_SET);

                    fread(all_data + current_offset, sizeof(char), file_size, file);
                    fclose(file);

                    // Debug stampa
                    // printf("Processo %d: Letto file %s, dimensione %ld\n", rank, filepath, file_size);

                    // Aggiorna l'offset corrente
                    current_offset += file_size;

                    // Aggiungi uno spazio tra i contenuti dei diversi file
                    if (current_offset < total_size)
                    {
                        all_data[current_offset] = ' ';
                        current_offset++;
                    }
                }
            }
        }
        closedir(dir);
        long avg_partition_size = total_size / (num_processes - 1);
        long extra_bytes = total_size % (num_processes - 1);
        //printf("Processo %d: Total size: %ld, Avg partition size: %ld, Extra bytes: %ld\n", rank, total_size, avg_partition_size, extra_bytes);
        for (int i = 1; i < num_processes; ++i)
        {
            long start_byte, end_byte;

            if (i == 1)
            {
                // Caso di invio a processo 1 (iniziale)
                start_byte = 0;
                end_byte = avg_partition_size + extra_bytes + 1;
            }
            else if (i < num_processes - 1)
            {
                // Caso di invio a processi intermedi
                start_byte = end_byte;
                end_byte = start_byte + avg_partition_size - 1;
            }
            else
            {
                // Caso di invio all'ultimo processo
                start_byte = end_byte;
                end_byte = total_size + 1;
            }

            // Aggiungi byte aggiunti per non troncare un'eventuale parola in lettura
            while (end_byte < total_size + 1 && all_data[end_byte] != ' ')
            {
                end_byte--;
            }

            // Calcola il numero effettivo di byte da inviare
            long byte_count = end_byte - start_byte + 1;

            // Invia start_byte, byte_count ai processi SLAVE
            //printf("Processo %d: Invia a processo %d - start_byte: %ld, end_byte: %ld , byte_count: %ld\n", rank, i, start_byte, end_byte, byte_count);
            MPI_Isend(&start_byte, 1, MPI_LONG, i, 0, MPI_COMM_WORLD, &request_send_size[i - 1]);
            MPI_Isend(&byte_count, 1, MPI_LONG, i, 0, MPI_COMM_WORLD, &request_send_buffer[i - 1]);
            MPI_Isend(&end_byte, 1, MPI_LONG, i, 0, MPI_COMM_WORLD, &request_send_buffer[i - 1]);
            MPI_Isend(all_data + start_byte, byte_count, MPI_CHAR, i, 0, MPI_COMM_WORLD, &request_send_buffer[i - 1]);
        }

        // Attesa del completamento degli invii asincroni
        MPI_Waitall(num_processes - 1, request_send_size, MPI_STATUSES_IGNORE);
        MPI_Waitall(num_processes - 1, request_send_buffer, MPI_STATUSES_IGNORE);

        initializeMasterHashtable(1);
        receiveAndMergeHistograms(num_processes);
       // Raccogli tutte le occorrenze in un array
    int totalOccurrences = 0;
    for (int i = 0; i < masterHashtable.size; ++i)
    {
        OccurrenceNode *current = masterHashtable.table[i];
        while (current != NULL)
        {
            totalOccurrences++;
            current = current->next;
        }
    }

   allOccurrencesArray = (OccurrenceNode *)malloc(totalOccurrences * sizeof(OccurrenceNode));
    int index = 0;

    for (int i = 0; i < masterHashtable.size; ++i)
    {
        OccurrenceNode *current = masterHashtable.table[i];
        while (current != NULL)
        {
            memcpy(&allOccurrencesArray[index], current, sizeof(OccurrenceNode));
            index++;
            current = current->next;
        }
    }

    // Ordina l'array di tutte le occorrenze
    qsort(allOccurrencesArray, totalOccurrences, sizeof(OccurrenceNode), compareOccurrences);

    // Chiamata alla funzione per scrivere il contenuto dell'array ordinato in un file CSV
    writeSortedArrayToCSV("istogramma_Globale_ordinato.csv", allOccurrencesArray, totalOccurrences);

    // Libera la memoria allocata per l'array di tutte le occorrenze
    free(allOccurrencesArray);
    }
    else
    {
        // SLAVE
        long start_byte, end_byte, byte_count;
        char *local_data;

        // Allocazione di local_occurrences
        local_occurrences = (OccurrenceNode *)malloc(total_size * sizeof(OccurrenceNode));

        // Ricevi start_byte, byte_count e end_byte dal processo MASTER
        MPI_Recv(&start_byte, 1, MPI_LONG, MASTER_RANK, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        MPI_Recv(&byte_count, 1, MPI_LONG, MASTER_RANK, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        MPI_Recv(&end_byte, 1, MPI_LONG, MASTER_RANK, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        // Allocazione di un buffer locale per contenere il chunk
        local_data = (char *)malloc(byte_count * sizeof(char));

        // Ricevi il chunk di dati dal processo MASTER
        MPI_Recv(local_data, byte_count, MPI_CHAR, MASTER_RANK, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        // printf("Processo %d: Ricevuto chunk: start_byte=%ld, end_byte=%ld, byte_count=%ld\n", rank, start_byte, end_byte, byte_count);

        // Inizializzazione della hashtable locale
        initializeHashtable(total_size + 1);

        // Inizializza parolecontate
        int parolecontate = 0;

        // Conteggio delle parole nel chunk locale
        char *delimiters = " .,'";
        char *saveptr;
        char *token = strtok_r(local_data, delimiters, &saveptr);
        //printf("TOKEN PRIMA DI WHILE: %s\n", token);
        while (token != NULL)
        {
            updateWordCount(token, rank);
            token = strtok_r(NULL, delimiters, &saveptr);
            //printf("TOKEN dopo DI WHILE: %s\n", token);
            parolecontate++;
        }

        // Verifica se ci sono parole contate prima di inviare
        if (parolecontate > 0)
        {
            // Array temporaneo per memorizzare i conteggi locali
            OccurrenceNode *local_occurrences_buffer = (OccurrenceNode *)malloc(parolecontate * sizeof(OccurrenceNode));

            // Copia i dati da local_occurrences a local_occurrences_buffer
            memcpy(local_occurrences_buffer, local_occurrences, parolecontate * sizeof(OccurrenceNode));

            LocalOccurrenceNode *current = local_occurrences_head;

            sendLocalOccurrences(rank);
            // Libera la memoria allocata per il buffer di invio
            free(local_occurrences_buffer);

            // Calcola la dimensione effettiva della hashtable locale
            int localHashtableSize = parolecontate;
            freeHashtable(); // Libera la memoria allocata precedentemente

            // Libera la memoria allocata per il buffer locale e di invio
            free(local_data);
            free(local_occurrences);
        }
    }
   /*
    end_time = MPI_Wtime();
    
    if(rank == 0)
        printf("Time in second = %f\n", end_time - start_time); */
    MPI_Finalize();

    return 0;
}