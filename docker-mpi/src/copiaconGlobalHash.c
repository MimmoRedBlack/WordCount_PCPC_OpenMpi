
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

// Funzione hash molto semplice (può essere migliorata)
unsigned int hashFunction(const char *word)
{
    unsigned int hash = 0;
    while (*word)
    {
        hash = (hash << 5) + *word++;
    }
    return hash;
}

void removePunctuation(char *word)
{
    int len = strlen(word);
    int j = 0;

    for (int i = 0; i < len; ++i)
    {
        if (isalpha(word[i]) || isdigit(word[i]))
        {
            word[j] = word[i];
            ++j;
        }
    }

    word[j] = '\0'; // Termina la stringa correttamente
}

void updateWordCount(const char *word, int rank)
{
    // Rimuovi la punteggiatura dalla parola
    char cleanedWord[MAX_WORD_LENGTH];
    strncpy(cleanedWord, word, MAX_WORD_LENGTH - 1);
    cleanedWord[MAX_WORD_LENGTH - 1] = '\0'; // Assicura che la stringa sia terminata correttamente
    removePunctuation(cleanedWord);

    // Calcola l'indice della hashtable utilizzando una funzione hash
    if (localHashtable.size == 0)
    {
        // La hashtable non è stata inizializzata correttamente
        printf("Processo %d: Hashtable non inizializzata correttamente.\n", rank);
        return;
    }

    unsigned int hashIndex = hashFunction(cleanedWord) % localHashtable.size;

    // Cerca la parola nella local_occurrences e incrementa il conteggio
    for (int i = 0; i < local_total_occurrences; ++i)
    {
        if (strcmp(local_occurrences[i].word, cleanedWord) == 0)
        {
            local_occurrences[i].count++;
            return; // Esci dalla funzione dopo l'aggiornamento
        }
    }

    // Cerca la parola nella lista di trabocco corrispondente
    OccurrenceNode *current = localHashtable.table[hashIndex];
    OccurrenceNode *prev = NULL;

    while (current != NULL)
    {
        if (strcmp(current->word, cleanedWord) == 0)
        {
            // La parola esiste nella hashtable, incrementa il conteggio
            current->count++;
            printf("Processo %d: Word %s found. Count updated to %d\n", rank, cleanedWord, current->count);
            return; // Esci dalla funzione dopo l'aggiornamento
        }

        prev = current;
        current = current->next;
    }

    // La parola non esiste, aggiungila alla lista di trabocco
    OccurrenceNode *newNode = (OccurrenceNode *)malloc(sizeof(OccurrenceNode));
    if (newNode == NULL)
    {
        // Gestisci l'errore di allocazione della memoria
        printf("Processo %d: Errore - Allocazione di memoria fallita.\n", rank);
        return;
    }

    newNode->count = 1; // Prima occorrenza
    strncpy(newNode->word, cleanedWord, MAX_WORD_LENGTH - 1);
    newNode->word[MAX_WORD_LENGTH - 1] = '\0'; // Assicura che la stringa sia terminata correttamente
    newNode->process_rank = rank;              // Aggiunto: memorizza il processo corrente
    newNode->next = NULL;

    // Aggiorna il puntatore alla testa della lista di trabocco
    if (prev != NULL)
    {
        prev->next = newNode;
    }
    else
    {
        localHashtable.table[hashIndex] = newNode;
    }

    // Aggiungi l'occorrenza dinamicamente all'array local_occurrences
    local_occurrences = (OccurrenceNode *)realloc(local_occurrences, (local_total_occurrences + 1) * sizeof(OccurrenceNode));
    if (local_occurrences == NULL)
    {
        // Gestisci l'errore di riallocazione della memoria
        printf("Processo %d: Errore - Riallocazione di memoria fallita.\n", rank);
        return;
    }

    // Aggiungi l'occorrenza solo se è una nuova parola
    local_occurrences[local_total_occurrences] = *newNode;
    local_total_occurrences++;
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

// Funzione per scrivere l'istogramma finale su un file CSV
void writeCSV(const char *filename, OccurrenceNode *histogram, int size)
{
    FILE *csvFile = fopen(filename, "w");
    if (csvFile == NULL)
    {
        perror("Errore nell'apertura del file CSV");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    // Intestazione del file CSV
    fprintf(csvFile, "Word,Count\n");

    // Scrittura dei dati
    for (int i = 0; i < size; ++i)
    {
        fprintf(csvFile, "%s,%d\n", histogram[i].word, histogram[i].count);
    }

    fclose(csvFile);
}
void initializeMasterHashtable(int size) {
    masterHashtable.size = size;

    // Stampa il valore di masterHashtable.size per debug
    printf("Master Hashtable Size: %d\n", masterHashtable.size);

    // Alloca dinamicamente la memoria per la tabella hash del master
    masterHashtable.table = (OccurrenceNode **)malloc(size * sizeof(OccurrenceNode *));

    // Inizializza tutti gli elementi della tabella hash a NULL
    for (int i = 0; i < size; ++i) {
        masterHashtable.table[i] = NULL;
    }
}

// Funzione per aggiornare l'istogramma globale nel processo MASTER
void updateGlobalHistogram(OccurrenceNode *localHistogram, int localHistogramSize, int totalParole)
{
    initializeMasterHashtable(totalParole);

    for (int i = 0; i < localHistogramSize; ++i)
    {
        // Rimuovi la punteggiatura dalla parola
        char cleanedWord[MAX_WORD_LENGTH];
        strncpy(cleanedWord, localHistogram[i].word, MAX_WORD_LENGTH - 1);
        cleanedWord[MAX_WORD_LENGTH - 1] = '\0';
        removePunctuation(cleanedWord);

        // Calcola l'indice della hashtable utilizzando una funzione hash
        unsigned int hashIndex = hashFunction(cleanedWord) % masterHashtable.size;

        // Stampa per debug
        printf("Aggiornamento istogramma globale - Parola: %s, Indice Hash: %u\n", cleanedWord, hashIndex);

        // Cerca la parola nella hashtable globale e aggiorna il conteggio
        OccurrenceNode *current = masterHashtable.table[hashIndex];
        while (current != NULL)
        {
            if (strcmp(current->word, cleanedWord) == 0)
            {
                // La parola esiste nella hashtable globale, incrementa il conteggio
                current->count += localHistogram[i].count;

                // Stampa per debug
                printf("Parola esistente. Aggiornato conteggio a %d\n", current->count);

                return; // Esci dalla funzione dopo l'aggiornamento
            }
            current = current->next;
        }

        // La parola non esiste nella hashtable globale, aggiungila
        OccurrenceNode *newNode = (OccurrenceNode *)malloc(sizeof(OccurrenceNode));
        if (newNode == NULL)
        {
            // Gestisci l'errore di allocazione della memoria
            printf("Processo MASTER: Errore - Allocazione di memoria fallita.\n");
            return;
        }

        // Copia i dati dalla localHistogram a newNode
        *newNode = localHistogram[i];

        // Aggiorna il puntatore alla testa della lista di trabocco
        newNode->next = masterHashtable.table[hashIndex];
        masterHashtable.table[hashIndex] = newNode;

        // Stampa per debug
        printf("Nuova parola aggiunta. Conteggio: %d\n", newNode->count);
    }
}

// Funzione per stampare la hashtable globale
void printGlobalHashtable()
{
    for (int i = 0; i < masterHashtable.size; ++i)
    {
        printf("Indice %d: ", i);
        OccurrenceNode *current = masterHashtable.table[i];
        while (current != NULL)
        {
            printf("(%s, %d) -> ", current->word, current->count);
            current = current->next;
        }
        printf("NULL\n");
    }
}

// Funzione per ricevere e aggregare gli istogrammi globali
void receiveAndMergeHistograms(int num_processes)
{
    MPI_Status status;

    // Ciclo fino a quando non si sono ricevuti tutti gli istogrammi
    for (int i = 1; i < num_processes; ++i)
    {
        int source_rank;
        MPI_Probe(MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
        source_rank = status.MPI_SOURCE;

        // Ottieni il numero di OccurrenceNode nel messaggio in arrivo
        int receivedCount;
        MPI_Get_count(&status, MPI_OCCURRENCE_NODE, &receivedCount);

        // Alloca un buffer per ospitare gli OccurrenceNode
        OccurrenceNode *receivedBuffer = (OccurrenceNode *)malloc(receivedCount * sizeof(OccurrenceNode));
        if (receivedBuffer == NULL)
        {
            // Gestisci l'errore di allocazione della memoria
            printf("Processo MASTER: Errore - Allocazione di memoria fallita.\n");
            MPI_Abort(MPI_COMM_WORLD, 1);
        }

        // Ricevi l'array di OccurrenceNode
        MPI_Recv(receivedBuffer, receivedCount, MPI_OCCURRENCE_NODE, source_rank, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        printf("received count MASTER: %d\n",receivedCount);
        int totalParole= receivedCount + totalParole;
        // Aggiorna l'istogramma globale con i dati ricevuti
        updateGlobalHistogram(receivedBuffer, receivedCount, totalParole);

        // Libera la memoria del buffer ricevuto
        free(receivedBuffer);
    }
        // Stampa la hashtable globale dopo tutti i merge
    printf("\nHashtabe globale dopo il merge:\n");
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
                    printf("Processo %d: Letto file %s, dimensione %ld\n", rank, filepath, file_size);

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
                end_byte = start_byte + avg_partition_size;
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
        receiveAndMergeHistograms(num_processes);

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
        printf("Processo %d: Allocazione buffer locale - start_byte: %ld, end_byte: %ld, byte_count: %ld\n", rank, start_byte, end_byte, byte_count);

        // Allocazione di un buffer locale per contenere il chunk
        local_data = (char *)malloc(byte_count * sizeof(char));

        // Debug stampa
        printf("Processo %d: Buffer locale allocato correttamente\n", rank);

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
            printf("Processo %d: Analisi parola: %s\n", rank, token);

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
            printf("Processo %d: Invio completato\n", rank);

            // Stampa di debug per verificare il contenuto di local_occurrences prima dell'invio
            printf("Processo %d: Contenuto di local_occurrences prima dell'invio:\n", rank);
            for (int i = 0; i < local_total_occurrences; ++i)
            {
                printf("local_total_occurrences: %d\n", local_total_occurrences);
                printf("CONTENUTO PRIMA DI INVIO Processo %d: Word %s, Count %d\n", rank, local_occurrences[i].word, local_occurrences[i].count);
            }
            // Invia le OccurrenceNode al processo MASTER
            MPI_Send(local_occurrences, local_total_occurrences, MPI_OCCURRENCE_NODE, MASTER_RANK, 0, MPI_COMM_WORLD);

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
