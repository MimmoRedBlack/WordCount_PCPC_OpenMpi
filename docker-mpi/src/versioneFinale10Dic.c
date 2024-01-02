#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <sys/stat.h>
#include <mpi.h>
#include <ctype.h>

#define MASTER_RANK 0
#define INPUT_DIR "./InputFile"
#define MAX_WORD_LENGTH 50 // Sostituisci con il valore appropriato

typedef struct OccurrenceNode
{
    int count;
    char word[MAX_WORD_LENGTH];
    int process_rank; // Aggiunto: per identificare il processo a cui appartiene
    int position;     // Aggiunto: per memorizzare la posizione della parola nella tabella hash
    struct OccurrenceNode *next;
} OccurrenceNode;

// Struttura per rappresentare la hashtable
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

// Dichiarazione della hashtable globale
Hashtable localHashtable;
// Dichiarazione della hashtable globale nel master
Hashtable masterHashtable;
OccurrenceNode *all_occurrences = NULL;
OccurrenceNode *local_occurrences = NULL;

int local_total_occurrences;

// Nuova struttura per rappresentare le occorrenze locali
typedef struct LocalOccurrenceNode
{
    char word[MAX_WORD_LENGTH];
    int count;
    struct LocalOccurrenceNode *next;
} LocalOccurrenceNode;

LocalOccurrenceNode *local_occurrences_head = NULL;

typedef struct GlobalOccurrenceNode
{
    char word[MAX_WORD_LENGTH];
    int count;
    struct GlobalOccurrenceNode *next;
} GlobalOccurrenceNode;

typedef struct
{
    unsigned int size;
    GlobalOccurrenceNode **table;
} GlobalHashtable;

int totalParole = 0;

// Dichiarazione della struttura MPI_OCCURRENCE_NODE
MPI_Datatype MPI_OCCURRENCE_NODE;

// Funzione per inizializzare la hashtable con una dimensione specifica
void initializeHashtable(unsigned int size)
{
    local_total_occurrences = 0;

    localHashtable.size = size;

    // Alloca dinamicamente la memoria per la tabella hash
    localHashtable.table = (OccurrenceNode **)malloc(size * sizeof(OccurrenceNode *));

    // Inizializza tutti gli elementi della tabella hash a NULL
    for (unsigned int i = 0; i < size; ++i)
    {
        localHashtable.table[i] = NULL;
    }
}

// Funzione per deallocare la memoria della hashtable
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

    // Dealloca la memoria utilizzata per l'array di occorrenze
    if (all_occurrences != NULL)
    {
        free(all_occurrences);
        all_occurrences = NULL; // Imposta il puntatore a NULL dopo la liberazione
    }
    if (local_occurrences != NULL)
    {
        free(local_occurrences);
        local_occurrences = NULL; // Imposta il puntatore a NULL dopo la liberazione
    }
}

void removePunctuation(char *word)
{
    // Rimuovi la punteggiatura dalla parola e converti le lettere a minuscolo
    int i, j = 0;
    for (i = 0; word[i]; ++i)
    {
        if (isalpha(word[i]))
        {
            word[j++] = tolower(word[i]);
        }
    }
    word[j] = '\0';
}

void updateWordCount(const char *word, int rank)
{
    // Rimuovi la punteggiatura dalla parola
    char cleanedWord[MAX_WORD_LENGTH];
    strncpy(cleanedWord, word, MAX_WORD_LENGTH - 1);
    cleanedWord[MAX_WORD_LENGTH - 1] = '\0'; // Assicura che la stringa sia terminata correttamente
    removePunctuation(cleanedWord);

    // Cerca la parola nella lista di occorrenze locali
    LocalOccurrenceNode *current = local_occurrences_head;
    while (current != NULL)
    {
        if (strcmp(current->word, cleanedWord) == 0)
        {
            // La parola esiste nella lista locale, incrementa il conteggio
            current->count++;
            printf("Processo %d: Word %s found. Count updated to %d\n", rank, cleanedWord, current->count);
            return; // Esci dalla funzione dopo l'aggiornamento
        }
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

    newNode->count = 1; // Prima occorrenza
    strncpy(newNode->word, cleanedWord, MAX_WORD_LENGTH - 1);
    newNode->word[MAX_WORD_LENGTH - 1] = '\0'; // Assicura che la stringa sia terminata correttamente
    newNode->next = local_occurrences_head;

    // Aggiorna il puntatore alla testa della lista locale
    local_occurrences_head = newNode;

    printf("Processo %d: Word %s added. Count set to 1\n", rank, cleanedWord);
}

long get_total_file_size(DIR *dir, const char *input_dir)
{
    long total_size = 0;
    struct dirent *entry;
    struct stat info;

    while ((entry = readdir(dir)) != NULL)
    {
        // Ignora le directory "." e ".."
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
        {
            continue;
        }

        char filepath[1024];
        snprintf(filepath, sizeof(filepath), "%s/%s", input_dir, entry->d_name);

        if (stat(filepath, &info) == 0)
        {
            total_size += info.st_size;
        }
    }

    rewinddir(dir);
    return total_size;
}

void printLocalHashtable(int rank)
{
    for (unsigned int i = 0; i < localHashtable.size; ++i)
    {
        OccurrenceNode *current = localHashtable.table[i];
        while (current != NULL)
        {
            // printf("Processo %d: Word %s, Count %d\n", rank, current->word, current->count);
            current = current->next;
        }
    }
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
        // Pulisci la parola prima di inviarla
        removePunctuation(current->word);

        MPI_Send(current, 1, MPI_OCCURRENCE_NODE, MASTER_RANK, 0, MPI_COMM_WORLD);
        current = current->next;
    }
}

// Funzione di confronto per il quicksort
int compareHistogramNodes(const void *a, const void *b)
{
    return ((OccurrenceNode *)b)->count - ((OccurrenceNode *)a)->count;
}

// Dichiarazione della funzione di confronto per qsort
int compareOccurrenceNodes(const void *a, const void *b)
{
    return ((OccurrenceNode *)b)->count - ((OccurrenceNode *)a)->count;
}

void writeCSV(const char *filename, Hashtable *hashTable)
{
    FILE *csvFile = fopen(filename, "w");
    if (csvFile == NULL)
    {
        perror("Errore nell'apertura del file CSV");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    // Intestazione del file CSV
    fprintf(csvFile, "Il numero finale delle occorrenze di ogni Word è il seguente.\n");

    // Scrittura dei dati dalla hashtable globale
    for (int i = 0; i < hashTable->size; ++i)
    {
        OccurrenceNode *current = hashTable->table[i];
        while (current != NULL)
        {
            // Pulisci la parola prima di scriverla
            char cleanedWord[MAX_WORD_LENGTH];
            strcpy(cleanedWord, current->word);
            removePunctuation(cleanedWord);

            // Scrivi nel file CSV
            fprintf(csvFile, "Word: %s - Count: %d\n", cleanedWord, current->count);

            current = current->next;
        }
    }

    fclose(csvFile);
}

void initializeMasterHashtable(int size)
{
    masterHashtable.size = size;

    // Stampa il valore di masterHashtable.size per debug
    printf("Master Hashtable Size: %d\n", masterHashtable.size);

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
    unsigned int hash = 5381;

    while (*word)
    {
        hash = ((hash << 5) + hash) + *word++; // Usando un mix di shift, somma e XOR
    }

    return (int)(hash % tableSize);
}

void updateGlobalHistogram(LocalOccurrenceNode *localHistogram, int localHistogramSize)
{

    LocalOccurrenceNode *currentNode = localHistogram;

    // Scorrere la lista di occorrenze locali
    while (currentNode != NULL)
    {
        char cleanedWord[MAX_WORD_LENGTH];
        strncpy(cleanedWord, currentNode->word, MAX_WORD_LENGTH - 1);
        cleanedWord[MAX_WORD_LENGTH - 1] = '\0';
        removePunctuation(cleanedWord);

        // Converti la parola a minuscolo
        for (int i = 0; cleanedWord[i]; i++)
        {
            cleanedWord[i] = tolower(cleanedWord[i]);
        }

        unsigned int hashIndex = hashFunction(cleanedWord, masterHashtable.size);

        printf("Aggiornamento istogramma globale - Parola: %s, Indice Hash: %u\n", cleanedWord, hashIndex);

        OccurrenceNode *current = masterHashtable.table[hashIndex];
        int wordFound = 0; // Flag che indica se la parola è stata trovata

        while (current != NULL)
        {
            if (strcmp(current->word, cleanedWord) == 0)
            {
                current->count += currentNode->count;
                printf("Parola esistente. Aggiornato conteggio a %d\n", current->count);
                wordFound = 1;
                break; // Termina il loop se la parola è già presente
            }
            current = current->next;
        }

        if (!wordFound)
        {
            // La parola non è stata trovata nella hashtable, quindi la aggiungi
            OccurrenceNode *newNode = (OccurrenceNode *)malloc(sizeof(OccurrenceNode));
            if (newNode == NULL)
            {
                printf("Processo MASTER: Errore - Allocazione di memoria fallita.\n");
                return;
            }

            newNode->count = currentNode->count;
            strncpy(newNode->word, cleanedWord, MAX_WORD_LENGTH - 1);
            newNode->word[MAX_WORD_LENGTH - 1] = '\0';

            newNode->next = masterHashtable.table[hashIndex];
            masterHashtable.table[hashIndex] = newNode;

            printf("Nuova parola aggiunta. Conteggio: %d\n", newNode->count);
        }

        currentNode = currentNode->next;
    }
    writeCSV("final_histogram.csv", &masterHashtable);
}

// Funzione per stampare la hashtable globale
void printGlobalHashtable()
{
    for (int i = 0; i < masterHashtable.size; ++i)
    {
        OccurrenceNode *current = masterHashtable.table[i];
        while (current != NULL)
        {
            // Pulisci la parola prima di stamparla
            char cleanedWord[MAX_WORD_LENGTH];
            strcpy(cleanedWord, current->word);
            removePunctuation(cleanedWord);

            printf("(%s, %d)\n", cleanedWord, current->count);
            current = current->next;
        }
    }
}

void printLocalOccurrences(LocalOccurrenceNode *head)
{
    LocalOccurrenceNode *current = head;

    while (current != NULL)
    {
        printf("Parola: %s, Count: %d\n", current->word, current->count);
        current = current->next;
    }
}

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
        printf("Processo MASTER: Ricevuto %d occorrenze locali dal processo %d.\n", receivedCount, source_rank);
        printLocalOccurrences(receivedBuffer); // Aggiungi una funzione di stampa per le occorrenze locali

        totalParole = +receivedCount; // Aggiorna il conteggio totale delle parole

        // Aggiorna l'istogramma globale con le occorrenze ricevute
        updateGlobalHistogram(receivedBuffer, receivedCount);
    }

    // Stampa la hashtable globale dopo tutti i merge
    printf("\nHashtable globale dopo il merge:\n");
    printGlobalHashtable();
}

int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);

    int rank, num_processes;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &num_processes);

    // Dichiarazioni e inizializzazioni necessarie
    MPI_Request request_send_size[num_processes - 1];
    MPI_Request request_send_buffer[num_processes - 1];
    MPI_Request request_receive_size[num_processes - 1];
    MPI_Request request_receive_buffer[num_processes - 1];
    MPI_Status status;
    MPI_Request *requests = (MPI_Request *)malloc((num_processes - 1) * sizeof(MPI_Request));

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

        printf("Processo %d: Total size: %ld, Avg partition size: %ld, Extra bytes: %ld\n", rank, total_size, avg_partition_size, extra_bytes);
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
                start_byte = end_byte + 1;
                end_byte = start_byte + avg_partition_size - 1;
            }
            else
            {
                // Caso di invio all'ultimo processo
                start_byte = end_byte + 1;
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
            printf("Processo %d: Invia a processo %d - start_byte: %ld, end_byte: %ld , byte_count: %ld\n", rank, i, start_byte, end_byte, byte_count);
            MPI_Isend(&start_byte, 1, MPI_LONG, i, 0, MPI_COMM_WORLD, &request_send_size[i - 1]);
            MPI_Isend(&byte_count, 1, MPI_LONG, i, 0, MPI_COMM_WORLD, &request_send_buffer[i - 1]);
            MPI_Isend(&end_byte, 1, MPI_LONG, i, 0, MPI_COMM_WORLD, &request_send_buffer[i - 1]);
            MPI_Isend(all_data + start_byte, byte_count, MPI_CHAR, i, 0, MPI_COMM_WORLD, &request_send_buffer[i - 1]);
        }

        // Attesa della completamento degli invii asincroni
        MPI_Waitall(num_processes - 1, request_send_size, MPI_STATUSES_IGNORE);
        MPI_Waitall(num_processes - 1, request_send_buffer, MPI_STATUSES_IGNORE);

        // Chiamata alla funzione per ricevere e aggregare gli istogrammi globali
        // valore di totalParole

        initializeMasterHashtable(1000);
        receiveAndMergeHistograms(num_processes);
        initializeMasterHashtable(totalParole);
        // Chiamata alla funzione per scrivere il contenuto della hashtable globale in un file CSV
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

        // Debug stampa
        // printf("Processo %d: Allocazione buffer locale - start_byte: %ld, end_byte: %ld, byte_count: %ld\n", rank, start_byte, end_byte, byte_count);

        // Allocazione di un buffer locale per contenere il chunk
        local_data = (char *)malloc(byte_count * sizeof(char));

        // Debug stampa
        //  printf("Processo %d: Buffer locale allocato correttamente\n", rank);

        // Ricevi il chunk di dati dal processo MASTER
        MPI_Recv(local_data, byte_count, MPI_CHAR, MASTER_RANK, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        // Stampa informazioni sul chunk ricevuto
        printf("Processo %d: Ricevuto chunk: start_byte=%ld, end_byte=%ld, byte_count=%ld\n", rank, start_byte, end_byte, byte_count);

        // Inizializzazione della hashtable locale
        initializeHashtable(total_size + 1);

        // Inizializza parolecontate
        int parolecontate = 0;

        // Conteggio delle parole nel chunk locale
        char *token = strtok(local_data, " "); // Separatore spazio
        while (token != NULL)
        {
            // Debug stampa
            // printf("Processo %d: Analisi parola: %s\n", rank, token);

            updateWordCount(token, rank);
            token = strtok(NULL, " ");
            parolecontate++;
        }

        // Verifica se ci sono parole contate prima di inviare
        if (parolecontate > 0)
        {
            // Array temporaneo per memorizzare i conteggi locali
            OccurrenceNode *local_occurrences_buffer = (OccurrenceNode *)malloc(parolecontate * sizeof(OccurrenceNode));

            // Copia i dati da local_occurrences a local_occurrences_buffer
            memcpy(local_occurrences_buffer, local_occurrences, parolecontate * sizeof(OccurrenceNode));

            // Debug stampa
            // printf("Processo %d: Invio completato\n", rank);

            // Stampa di debug per verificare il contenuto di local_occurrences prima dell'invio
            // printf("Processo %d: Contenuto di local_occurrences prima dell'invio:\n", rank);

            // Utilizza una struttura ciclica per scorrere la lista locale
            LocalOccurrenceNode *current = local_occurrences_head;
            /*while (current != NULL)
            {
                printf("local_total_occurrences: %d\n", current->count);
                printf("CONTENUTO PRIMA DI INVIO Processo %d: Word %s, Count %d\n", rank, current->word, current->count);
                current = current->next;
            }*/

            sendLocalOccurrences(rank);
            // Stampa il contenuto della hashtable locale
            printLocalHashtable(rank);
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
    MPI_Finalize();
    return 0;
}

___________________________

12 Dic corretto :

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <sys/stat.h>
#include <mpi.h>
#include <ctype.h>

#define MASTER_RANK 0
#define INPUT_DIR "./InputFile"
#define MAX_WORD_LENGTH 50 // Sostituisci con il valore appropriato

    typedef struct OccurrenceNode
{
    int count;
    char word[MAX_WORD_LENGTH];
    int process_rank; // Aggiunto: per identificare il processo a cui appartiene
    int position;     // Aggiunto: per memorizzare la posizione della parola nella tabella hash
    struct OccurrenceNode *next;
} OccurrenceNode;

// Struttura per rappresentare la hashtable
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

// Dichiarazione della hashtable globale
Hashtable localHashtable;
// Dichiarazione della hashtable globale nel master
Hashtable masterHashtable;
OccurrenceNode *all_occurrences = NULL;
OccurrenceNode *local_occurrences = NULL;

int local_total_occurrences;

// Nuova struttura per rappresentare le occorrenze locali
typedef struct LocalOccurrenceNode
{
    char word[MAX_WORD_LENGTH];
    int count;
    struct LocalOccurrenceNode *next;
} LocalOccurrenceNode;

LocalOccurrenceNode *local_occurrences_head = NULL;

typedef struct GlobalOccurrenceNode
{
    char word[MAX_WORD_LENGTH];
    int count;
    struct GlobalOccurrenceNode *next;
} GlobalOccurrenceNode;

typedef struct
{
    unsigned int size;
    GlobalOccurrenceNode **table;
} GlobalHashtable;

int totalParole = 0;

// Dichiarazione della struttura MPI_OCCURRENCE_NODE
MPI_Datatype MPI_OCCURRENCE_NODE;

// Funzione per inizializzare la hashtable con una dimensione specifica
void initializeHashtable(unsigned int size)
{
    local_total_occurrences = 0;

    localHashtable.size = size;

    // Alloca dinamicamente la memoria per la tabella hash
    localHashtable.table = (OccurrenceNode **)malloc(size * sizeof(OccurrenceNode *));

    // Inizializza tutti gli elementi della tabella hash a NULL
    for (unsigned int i = 0; i < size; ++i)
    {
        localHashtable.table[i] = NULL;
    }
}

// Funzione per deallocare la memoria della hashtable
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

    // Dealloca la memoria utilizzata per l'array di occorrenze
    if (all_occurrences != NULL)
    {
        free(all_occurrences);
        all_occurrences = NULL; // Imposta il puntatore a NULL dopo la liberazione
    }
    if (local_occurrences != NULL)
    {
        free(local_occurrences);
        local_occurrences = NULL; // Imposta il puntatore a NULL dopo la liberazione
    }
}

void removePunctuation(char *word)
{
    // Rimuovi la punteggiatura dalla parola e converti le lettere a minuscolo
    int i, j = 0;
    for (i = 0; word[i]; ++i)
    {
        if (isalpha(word[i]))
        {
            word[j++] = tolower(word[i]);
        }
    }
    word[j] = '\0';
}

void updateWordCount(const char *word, int rank)
{
    // Rimuovi la punteggiatura dalla parola
    char cleanedWord[MAX_WORD_LENGTH];
    strncpy(cleanedWord, word, MAX_WORD_LENGTH - 1);
    cleanedWord[MAX_WORD_LENGTH - 1] = '\0'; // Assicura che la stringa sia terminata correttamente
    removePunctuation(cleanedWord);

    // Cerca la parola nella lista di occorrenze locali
    LocalOccurrenceNode *current = local_occurrences_head;
    while (current != NULL)
    {
        if (strcmp(current->word, cleanedWord) == 0)
        {
            // La parola esiste nella lista locale, incrementa il conteggio
            current->count++;
            printf("Processo %d: Word %s found. Count updated to %d\n", rank, cleanedWord, current->count);
            return; // Esci dalla funzione dopo l'aggiornamento
        }
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

    newNode->count = 1; // Prima occorrenza
    strncpy(newNode->word, cleanedWord, MAX_WORD_LENGTH - 1);
    newNode->word[MAX_WORD_LENGTH - 1] = '\0'; // Assicura che la stringa sia terminata correttamente
    newNode->next = local_occurrences_head;

    // Aggiorna il puntatore alla testa della lista locale
    local_occurrences_head = newNode;

    printf("Processo %d: Word %s added. Count set to 1\n", rank, cleanedWord);
}

long get_total_file_size(DIR *dir, const char *input_dir)
{
    long total_size = 0;
    struct dirent *entry;
    struct stat info;

    while ((entry = readdir(dir)) != NULL)
    {
        // Ignora le directory "." e ".."
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
        {
            continue;
        }

        char filepath[1024];
        snprintf(filepath, sizeof(filepath), "%s/%s", input_dir, entry->d_name);

        if (stat(filepath, &info) == 0)
        {
            total_size += info.st_size;
        }
    }

    rewinddir(dir);
    return total_size;
}

void printLocalHashtable(int rank)
{
    for (unsigned int i = 0; i < localHashtable.size; ++i)
    {
        OccurrenceNode *current = localHashtable.table[i];
        while (current != NULL)
        {
            // printf("Processo %d: Word %s, Count %d\n", rank, current->word, current->count);
            current = current->next;
        }
    }
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
        // Pulisci la parola prima di inviarla
        removePunctuation(current->word);

        MPI_Send(current, 1, MPI_OCCURRENCE_NODE, MASTER_RANK, 0, MPI_COMM_WORLD);
        current = current->next;
    }
}

// Funzione di confronto per il quicksort
int compareHistogramNodes(const void *a, const void *b)
{
    return ((OccurrenceNode *)b)->count - ((OccurrenceNode *)a)->count;
}

// Dichiarazione della funzione di confronto per qsort
int compareOccurrenceNodes(const void *a, const void *b)
{
    return ((OccurrenceNode *)b)->count - ((OccurrenceNode *)a)->count;
}

void writeCSV(const char *filename, Hashtable *hashTable)
{
    FILE *csvFile = fopen(filename, "w");
    if (csvFile == NULL)
    {
        perror("Errore nell'apertura del file CSV");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    // Intestazione del file CSV
    fprintf(csvFile, "Il numero finale delle occorrenze di ogni Word è il seguente.\n");

    // Scrittura dei dati dalla hashtable globale
    for (int i = 0; i < hashTable->size; ++i)
    {
        OccurrenceNode *current = hashTable->table[i];
        while (current != NULL)
        {
            // Pulisci la parola prima di scriverla
            char cleanedWord[MAX_WORD_LENGTH];
            strcpy(cleanedWord, current->word);
            removePunctuation(cleanedWord);

            // Scrivi nel file CSV
            fprintf(csvFile, "Word: %s - Count: %d\n", cleanedWord, current->count);

            current = current->next;
        }
    }

    fclose(csvFile);
}

void initializeMasterHashtable(int size)
{
    masterHashtable.size = size;

    // Stampa il valore di masterHashtable.size per debug
    printf("Master Hashtable Size: %d\n", masterHashtable.size);

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
    unsigned int hash = 5381;

    while (*word)
    {
        hash = ((hash << 5) + hash) + *word++; // Usando un mix di shift, somma e XOR
    }

    return (int)(hash % tableSize);
}

void updateGlobalHistogram(LocalOccurrenceNode *localHistogram, int localHistogramSize)
{

    LocalOccurrenceNode *currentNode = localHistogram;

    // Scorrere la lista di occorrenze locali
    while (currentNode != NULL)
    {
        char cleanedWord[MAX_WORD_LENGTH];
        strncpy(cleanedWord, currentNode->word, MAX_WORD_LENGTH - 1);
        cleanedWord[MAX_WORD_LENGTH - 1] = '\0';
        removePunctuation(cleanedWord);

        // Converti la parola a minuscolo
        for (int i = 0; cleanedWord[i]; i++)
        {
            cleanedWord[i] = tolower(cleanedWord[i]);
        }

        unsigned int hashIndex = hashFunction(cleanedWord, masterHashtable.size);

        printf("Aggiornamento istogramma globale - Parola: %s, Indice Hash: %u\n", cleanedWord, hashIndex);

        OccurrenceNode *current = masterHashtable.table[hashIndex];
        int wordFound = 0; // Flag che indica se la parola è stata trovata

        while (current != NULL)
        {
            if (strcmp(current->word, cleanedWord) == 0)
            {
                current->count += currentNode->count;
                printf("Parola esistente. Aggiornato conteggio a %d\n", current->count);
                wordFound = 1;
                break; // Termina il loop se la parola è già presente
            }
            current = current->next;
        }

        if (!wordFound)
        {
            // La parola non è stata trovata nella hashtable, quindi la aggiungi
            OccurrenceNode *newNode = (OccurrenceNode *)malloc(sizeof(OccurrenceNode));
            if (newNode == NULL)
            {
                printf("Processo MASTER: Errore - Allocazione di memoria fallita.\n");
                return;
            }

            newNode->count = currentNode->count;
            strncpy(newNode->word, cleanedWord, MAX_WORD_LENGTH - 1);
            newNode->word[MAX_WORD_LENGTH - 1] = '\0';

            newNode->next = masterHashtable.table[hashIndex];
            masterHashtable.table[hashIndex] = newNode;

            printf("Nuova parola aggiunta. Conteggio: %d\n", newNode->count);
        }

        currentNode = currentNode->next;
    }
    writeCSV("final_histogram.csv", &masterHashtable);
}

// Funzione per stampare la hashtable globale
void printGlobalHashtable()
{
    for (int i = 0; i < masterHashtable.size; ++i)
    {
        OccurrenceNode *current = masterHashtable.table[i];
        while (current != NULL)
        {
            // Pulisci la parola prima di stamparla
            char cleanedWord[MAX_WORD_LENGTH];
            strcpy(cleanedWord, current->word);
            removePunctuation(cleanedWord);

            printf("(%s, %d)\n", cleanedWord, current->count);
            current = current->next;
        }
    }
}

void printLocalOccurrences(LocalOccurrenceNode *head)
{
    LocalOccurrenceNode *current = head;

    while (current != NULL)
    {
        printf("Parola: %s, Count: %d\n", current->word, current->count);
        current = current->next;
    }
}

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
        printf("Processo MASTER: Ricevuto %d occorrenze locali dal processo %d.\n", receivedCount, source_rank);
        printLocalOccurrences(receivedBuffer); // Aggiungi una funzione di stampa per le occorrenze locali
        totalParole += receivedCount;          // Aggiorna il conteggio totale delle parole

        // Aggiorna l'istogramma globale con le occorrenze ricevute
        updateGlobalHistogram(receivedBuffer, receivedCount);
    }

    // Stampa la hashtable globale dopo tutti i merge
    printf("\nHashtable globale dopo il merge:\n");
    printGlobalHashtable();
}

int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);

    int rank, num_processes;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &num_processes);

    // Dichiarazioni e inizializzazioni necessarie
    MPI_Request request_send_size[num_processes - 1];
    MPI_Request request_send_buffer[num_processes - 1];
    MPI_Request request_receive_size[num_processes - 1];
    MPI_Request request_receive_buffer[num_processes - 1];
    MPI_Status status;
    MPI_Request *requests = (MPI_Request *)malloc((num_processes - 1) * sizeof(MPI_Request));

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

        printf("Processo %d: Total size: %ld, Avg partition size: %ld, Extra bytes: %ld\n", rank, total_size, avg_partition_size, extra_bytes);
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
                start_byte = end_byte + 1;
                end_byte = start_byte + avg_partition_size - 1;
            }
            else
            {
                // Caso di invio all'ultimo processo
                start_byte = end_byte + 1;
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
            printf("Processo %d: Invia a processo %d - start_byte: %ld, end_byte: %ld , byte_count: %ld\n", rank, i, start_byte, end_byte, byte_count);
            MPI_Isend(&start_byte, 1, MPI_LONG, i, 0, MPI_COMM_WORLD, &request_send_size[i - 1]);
            MPI_Isend(&byte_count, 1, MPI_LONG, i, 0, MPI_COMM_WORLD, &request_send_buffer[i - 1]);
            MPI_Isend(&end_byte, 1, MPI_LONG, i, 0, MPI_COMM_WORLD, &request_send_buffer[i - 1]);
            MPI_Isend(all_data + start_byte, byte_count, MPI_CHAR, i, 0, MPI_COMM_WORLD, &request_send_buffer[i - 1]);
        }

        // Attesa della completamento degli invii asincroni
        MPI_Waitall(num_processes - 1, request_send_size, MPI_STATUSES_IGNORE);
        MPI_Waitall(num_processes - 1, request_send_buffer, MPI_STATUSES_IGNORE);

        // Chiamata alla funzione per ricevere e aggregare gli istogrammi globali
        // valore di totalParole

        initializeMasterHashtable(1000000);
        receiveAndMergeHistograms(num_processes);
        initializeMasterHashtable(totalParole);
        // Chiamata alla funzione per scrivere il contenuto della hashtable globale in un file CSV
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

        // Debug stampa
        // printf("Processo %d: Allocazione buffer locale - start_byte: %ld, end_byte: %ld, byte_count: %ld\n", rank, start_byte, end_byte, byte_count);

        // Allocazione di un buffer locale per contenere il chunk
        local_data = (char *)malloc(byte_count * sizeof(char));

        // Debug stampa
        //  printf("Processo %d: Buffer locale allocato correttamente\n", rank);

        // Ricevi il chunk di dati dal processo MASTER
        MPI_Recv(local_data, byte_count, MPI_CHAR, MASTER_RANK, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        // Stampa informazioni sul chunk ricevuto
        printf("Processo %d: Ricevuto chunk: start_byte=%ld, end_byte=%ld, byte_count=%ld\n", rank, start_byte, end_byte, byte_count);

        // Inizializzazione della hashtable locale
        initializeHashtable(total_size + 1);

        // Inizializza parolecontate
        int parolecontate = 0;

        // Conteggio delle parole nel chunk locale
        char *delimiters = " .,";
        char *saveptr;
        char *token = strtok_r(local_data, delimiters, &saveptr);
        printf("TOKEN PRIMA DI WHILE: %s\n", token);
        while (token != NULL)
        {
            // Debug stampa
            // printf("Processo %d: Analisi parola: %s\n", rank, token);

            updateWordCount(token, rank);
            token = strtok_r(NULL, delimiters, &saveptr);
            printf("TOKEN dopo DI WHILE: %s\n", token);
            parolecontate++;
        }

        // Verifica se ci sono parole contate prima di inviare
        if (parolecontate > 0)
        {
            // Array temporaneo per memorizzare i conteggi locali
            OccurrenceNode *local_occurrences_buffer = (OccurrenceNode *)malloc(parolecontate * sizeof(OccurrenceNode));

            // Copia i dati da local_occurrences a local_occurrences_buffer
            memcpy(local_occurrences_buffer, local_occurrences, parolecontate * sizeof(OccurrenceNode));

            // Debug stampa
            // printf("Processo %d: Invio completato\n", rank);

            // Stampa di debug per verificare il contenuto di local_occurrences prima dell'invio
            // printf("Processo %d: Contenuto di local_occurrences prima dell'invio:\n", rank);

            // Utilizza una struttura ciclica per scorrere la lista locale
            LocalOccurrenceNode *current = local_occurrences_head;
            /*while (current != NULL)
            {
                printf("local_total_occurrences: %d\n", current->count);
                printf("CONTENUTO PRIMA DI INVIO Processo %d: Word %s, Count %d\n", rank, current->word, current->count);
                current = current->next;
            }*/

            sendLocalOccurrences(rank);
            // Libera la memoria allocata per il buffer di invio
            free(local_occurrences_buffer);

            // Stampa il contenuto della hashtable locale
            printLocalHashtable(rank);

            // Calcola la dimensione effettiva della hashtable locale
            int localHashtableSize = parolecontate;
            freeHashtable(); // Libera la memoria allocata precedentemente

            // Libera la memoria allocata per il buffer locale e di invio
            free(local_data);
            free(local_occurrences);
        }
    }
    MPI_Finalize();
    return 0;
}






































++++++++++++++++++++++++
27 Dicembre

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
    int process_rank; // id processo a cui appartiene
    int position;     // posizione della parola nella tab hash
    struct OccurrenceNode *next;
} OccurrenceNode;

// Struttura hashtable
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

Hashtable localHashtable;
Hashtable masterHashtable;
OccurrenceNode *all_occurrences = NULL;
OccurrenceNode *local_occurrences = NULL;

// Nuova struttura per rappresentare le occorrenze locali
typedef struct LocalOccurrenceNode
{
    char word[MAX_WORD_LENGTH];
    int count;
    struct LocalOccurrenceNode *next;
} LocalOccurrenceNode;

LocalOccurrenceNode *local_occurrences_head = NULL;

typedef struct GlobalOccurrenceNode
{
    char word[MAX_WORD_LENGTH];
    int count;
    struct GlobalOccurrenceNode *next;
} GlobalOccurrenceNode;

typedef struct
{
    unsigned int size;
    GlobalOccurrenceNode **table;
} GlobalHashtable;

int totalParole = 0;

// Dichiarazione del tipo MPI_OCCURRENCE_NODE
MPI_Datatype MPI_OCCURRENCE_NODE;

// Funzione per inizializzare la hashtable con una dimensione specifica
void initializeHashtable(unsigned int size)
{
    localHashtable.size = size;
    localHashtable.table = (OccurrenceNode **)malloc(size * sizeof(OccurrenceNode *));

    // Inizializzo elementi a NULL
    for (unsigned int i = 0; i < size; ++i)
    {
        localHashtable.table[i] = NULL;
    }
}

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

    // Dealloca la memoria utilizzata per l'array di occorrenze
    if (all_occurrences != NULL)
    {
        free(all_occurrences);
        all_occurrences = NULL; // Imposta il puntatore a NULL dopo la liberazione
    }
    if (local_occurrences != NULL)
    {
        free(local_occurrences);
        local_occurrences = NULL; // Imposta il puntatore a NULL dopo la liberazione
    }
}


void removePunctuation(char *word)
{
    // Rimuove la punteggiatura dalla parola e converte a lower.
    int i, j = 0;
    for (i = 0; word[i]; ++i)
    {
        if (isalpha(word[i]))
        {
            word[j++] = tolower(word[i]);
        }
    }
    word[j] = '\0';
}

void updateWordCount(const char *word, int rank)
{
    // Rimoziobe della punteggiatura dalla parola
    char cleanedWord[MAX_WORD_LENGTH];
    strncpy(cleanedWord, word, MAX_WORD_LENGTH - 1);
    cleanedWord[MAX_WORD_LENGTH - 1] = '\0'; // Assicura che la stringa sia terminata correttamente
    removePunctuation(cleanedWord);

    // Cerca la parola nella lista di occorrenze locali
    LocalOccurrenceNode *current = local_occurrences_head;
    while (current != NULL)
    {
        if (strcmp(current->word, cleanedWord) == 0)
        {
            // se esiste nella lista locale, incremento il conteggio
            current->count++;
            printf("Processo %d: Word %s , Count aggiornayo a %d\n", rank, cleanedWord, current->count);
            return;
        }
        current = current->next;
    }

    // se non esiste nella lista locale la aggiungp
    LocalOccurrenceNode *newNode = (LocalOccurrenceNode *)malloc(sizeof(LocalOccurrenceNode));
    if (newNode == NULL)
    {
        printf("Processo %d: Errore - Allocazione di memoria fallita.\n", rank);
        return;
    }

    newNode->count = 1; // Prima occorrenza
    strncpy(newNode->word, cleanedWord, MAX_WORD_LENGTH - 1);
    newNode->word[MAX_WORD_LENGTH - 1] = '\0'; // stringa terminata correttamente
    newNode->next = local_occurrences_head;

    // Aggiorno il puntatore alla testa della lista locale
    local_occurrences_head = newNode;

    printf("Processo %d: Word %s aggiunta. Count: 1\n", rank, cleanedWord);
}

long get_total_file_size(DIR *dir, const char *input_dir)
{
    long total_size = 0;
    struct dirent *entry;
    struct stat info;

    while ((entry = readdir(dir)) != NULL)
    {
        // Ignora le directory "." e ".."
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
        {
            continue;
        }

        char filepath[1024];
        snprintf(filepath, sizeof(filepath), "%s/%s", input_dir, entry->d_name);

        if (stat(filepath, &info) == 0)
        {
            total_size += info.st_size;
        }
    }

    rewinddir(dir);
    return total_size;
}

void printLocalHashtable(int rank)
{
    for (unsigned int i = 0; i < localHashtable.size; ++i)
    {
        OccurrenceNode *current = localHashtable.table[i];
        while (current != NULL)
        {
            current = current->next;
        }
    }
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
        // Pulisci la parola prima di inviarla
        removePunctuation(current->word);

        MPI_Send(current, 1, MPI_OCCURRENCE_NODE, MASTER_RANK, 0, MPI_COMM_WORLD);
        current = current->next;
    }
    free(local_occurrences_head);
}

void writeCSV(const char *filename, Hashtable *hashTable)
{
    FILE *csvFile = fopen(filename, "w");
    if (csvFile == NULL)
    {
        perror("Errore nell'apertura del file CSV");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    fprintf(csvFile, "Il numero finale delle occorrenze di ogni Word è il seguente.\n");

    // Scrittura dei dati dalla hashtable globale
    for (int i = 0; i < hashTable->size; ++i)
    {
        OccurrenceNode *current = hashTable->table[i];
        while (current != NULL)
        {
            // Pulizia dellla parola prima di scriverla
            char cleanedWord[MAX_WORD_LENGTH];
            strcpy(cleanedWord, current->word);
            removePunctuation(cleanedWord);

            // Scrivi nel file CSV
            fprintf(csvFile, "Word: %s - Count: %d\n", cleanedWord, current->count);

            current = current->next;
        }
    }

    fclose(csvFile);
}

void initializeMasterHashtable(int size)
{
    masterHashtable.size = size;

    // Allocazione dinamica memoria tabella hash del master
    masterHashtable.table = (OccurrenceNode **)malloc(size * sizeof(OccurrenceNode *));

    // Iniz a NULL
    for (int i = 0; i < size; ++i)
    {
        masterHashtable.table[i] = NULL;
    }
}

int hashFunction(const char *word, int tableSize)
{
    unsigned int hash = 5381;

    while (*word)
    {
        hash = ((hash << 5) + hash) + *word++; // shift, somma e XOR
    }

    return (int)(hash % tableSize);
}

void updateGlobalHistogram(LocalOccurrenceNode *localHistogram, int localHistogramSize)
{

    LocalOccurrenceNode *currentNode = localHistogram;

    // Scorrere la lista di occorrenze locali
    while (currentNode != NULL)
    {
        char cleanedWord[MAX_WORD_LENGTH];
        strncpy(cleanedWord, currentNode->word, MAX_WORD_LENGTH - 1);
        cleanedWord[MAX_WORD_LENGTH - 1] = '\0';
        removePunctuation(cleanedWord);

        // parola a minuscolo
        for (int i = 0; cleanedWord[i]; i++)
        {
            cleanedWord[i] = tolower(cleanedWord[i]);
        }

        unsigned int hashIndex = hashFunction(cleanedWord, masterHashtable.size);

        // printf("Aggiornamento istogramma globale - Parola: %s, Indice Hash: %u\n", cleanedWord, hashIndex);

        OccurrenceNode *current = masterHashtable.table[hashIndex];
        int wordFound = 0; // indica se la parola è stata trovata

        while (current != NULL)
        {
            if (strcmp(current->word, cleanedWord) == 0)
            {
                current->count += currentNode->count;
                // printf("Parola gia esistente. Aggiornato conteggio a %d\n", current->count);
                wordFound = 1;
                break; // Termina il loop se la parola è già presente
            }
            current = current->next;
        }

        if (!wordFound)
        {
            // se la parola non è stata trovata nella hashtable viene agg.
            OccurrenceNode *newNode = (OccurrenceNode *)malloc(sizeof(OccurrenceNode));
            if (newNode == NULL)
            {
                printf("Processo MASTER: Errore - Allocazione di memoria fallita.\n");
                return;
            }

            newNode->count = currentNode->count;
            strncpy(newNode->word, cleanedWord, MAX_WORD_LENGTH - 1);
            newNode->word[MAX_WORD_LENGTH - 1] = '\0';

            newNode->next = masterHashtable.table[hashIndex];
            masterHashtable.table[hashIndex] = newNode;

            // printf("Nuova parola aggiunta. Conteggio: %d\n", newNode->count);
        }

        currentNode = currentNode->next;
    }
    writeCSV("final_histogram.csv", &masterHashtable);
}

// Funzione per stampare la hashtable globale
void printGlobalHashtable()
{
    for (int i = 0; i < masterHashtable.size; ++i)
    {
        OccurrenceNode *current = masterHashtable.table[i];
        while (current != NULL)
        {
            // Pulisci la parola prima di stamparla
            char cleanedWord[MAX_WORD_LENGTH];
            strcpy(cleanedWord, current->word);
            removePunctuation(cleanedWord);

            printf("(%s, %d)\n", cleanedWord, current->count);
            current = current->next;
        }
    }
}

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
        totalParole = +receivedCount; // Aggiorna il conteggio totale delle parole

        // Aggiorna l'istogramma globale con le occorrenze ricevute
        updateGlobalHistogram(receivedBuffer, receivedCount);
    }
    printf("\nHashtable globale dopo il merge:\n");
    printGlobalHashtable();
}

int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);

    int rank, num_processes;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &num_processes);

    // Dichiarazioni e inizializzazioni necessarie
    MPI_Request request_send_size[num_processes - 1];
    MPI_Request request_send_buffer[num_processes - 1];
    MPI_Request request_receive_size[num_processes - 1];
    MPI_Request request_receive_buffer[num_processes - 1];
    MPI_Status status;
    MPI_Request *requests = (MPI_Request *)malloc((num_processes - 1) * sizeof(MPI_Request));

    // Dichiarazione delle variabili per MPI
    int blockcounts[2] = {1, MAX_WORD_LENGTH};
    MPI_Aint offsets[2] = {offsetof(OccurrenceNode, count), offsetof(OccurrenceNode, word)};
    MPI_Datatype types[2] = {MPI_INT, MPI_CHAR};

    MPI_Type_create_struct(2, blockcounts, offsets, types, &MPI_OCCURRENCE_NODE);
    MPI_Type_commit(&MPI_OCCURRENCE_NODE);

    DIR *dir = opendir(INPUT_DIR);
    if (!dir)
    {
        perror("Errore nell'apertura della directory");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    long total_size = get_total_file_size(dir, INPUT_DIR);

    // Invia la dimensione totale del file a tutti i processi
    MPI_Bcast(&total_size, 1, MPI_LONG, MASTER_RANK, MPI_COMM_WORLD);

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

        // Leggo tutti i dati dai file nella directory nel buffer all_data
        long current_offset = 0;
        while ((entry = readdir(dir)) != NULL)
        {
            if (entry->d_type == DT_REG)
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

        printf("Processo %d: Total size: %ld, Avg partition size: %ld, Extra bytes: %ld\n", rank, total_size, avg_partition_size, extra_bytes);
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
                start_byte = end_byte + 1;
                end_byte = start_byte + avg_partition_size - 1;
            }
            else
            {
                // Caso di invio all'ultimo processo
                start_byte = end_byte + 1;
                end_byte = total_size + 1;
            }

            // decremento byte per non troncare lettura
            while (end_byte < total_size + 1 && all_data[end_byte] != ' ')
            {
                end_byte--;
            }

            // Calcola il numero effettivo di byte da inviare
            long byte_count = end_byte - start_byte + 1;

            // Invia start_byte, byte_count ai processi SLAVE
            printf("Processo %d: Invia a processo %d - start_byte: %ld, end_byte: %ld , byte_count: %ld\n", rank, i, start_byte, end_byte, byte_count);
            MPI_Isend(&start_byte, 1, MPI_LONG, i, 0, MPI_COMM_WORLD, &request_send_size[i - 1]);
            MPI_Isend(&byte_count, 1, MPI_LONG, i, 0, MPI_COMM_WORLD, &request_send_buffer[i - 1]);
            MPI_Isend(&end_byte, 1, MPI_LONG, i, 0, MPI_COMM_WORLD, &request_send_buffer[i - 1]);
            MPI_Isend(all_data + start_byte, byte_count, MPI_CHAR, i, 0, MPI_COMM_WORLD, &request_send_buffer[i - 1]);
        }

        // Attesa della completamento degli invii asincroni
        MPI_Waitall(num_processes - 1, request_send_size, MPI_STATUSES_IGNORE);
        MPI_Waitall(num_processes - 1, request_send_buffer, MPI_STATUSES_IGNORE);

        initializeMasterHashtable(1000000);
        // Chiamata alla funzione per ricevere e aggregare gli istogrammi globali
        receiveAndMergeHistograms(num_processes);
        //initializeMasterHashtable(totalParole);
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
        //  printf("Processo %d: Buffer locale allocato correttamente\n", rank);

        // Ricevi il chunk di dati dal processo MASTER
        MPI_Recv(local_data, byte_count, MPI_CHAR, MASTER_RANK, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        // printf("Processo %d: Ricevuto chunk: start_byte=%ld, end_byte=%ld, byte_count=%ld\n", rank, start_byte, end_byte, byte_count);

        // Inizializzazione della hashtable locale
        initializeHashtable(total_size + 1);

        // Inizializza parolecontate
        int parolecontate = 0;

        // Conteggio delle parole nel chunk locale
        char *delimiters = " .,";
        char *saveptr;
        char *token = strtok_r(local_data, delimiters, &saveptr);
        // printf("TOKEN PRIMA DI WHILE: %s\n",token);
        while (token != NULL)
        {
            // printf("Processo %d: Analisi parola: %s\n", rank, token);
            updateWordCount(token, rank);
            token = strtok_r(NULL, delimiters, &saveptr);
            // printf("TOKEN dopo DI WHILE: %s\n",token);
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

            // Stampa il contenuto della hashtable locale
            printLocalHashtable(rank);

            // Calcola la dimensione effettiva della hashtable locale
            int localHashtableSize = parolecontate;
            freeHashtable(); // Libera la memoria allocata precedentemente

            // Libera la memoria allocata per il buffer locale e di invio
            free(local_data);
        }
    }

    MPI_Finalize();
    return 0;
}