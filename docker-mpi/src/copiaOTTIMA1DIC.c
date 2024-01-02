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

// Struttura per rappresentare un elemento nella lista di trabocco
typedef struct OccurrenceNode
{
    int count;
    char word[MAX_WORD_LENGTH];
    struct OccurrenceNode *next;
} OccurrenceNode;

// Struttura per rappresentare la hashtable
typedef struct
{
    unsigned int size;
    OccurrenceNode **table;
} Hashtable;

// Dichiarazione della hashtable globale
Hashtable globalHashtable;
int total_occurrences = 0;
int local_total_occurrences;
// Dichiarazione della struttura MPI_OCCURRENCE_NODE
MPI_Datatype MPI_OCCURRENCE_NODE;

// Funzione per inizializzare la hashtable con una dimensione specifica
void initializeHashtable(unsigned int size)
{
    globalHashtable.size = size;

    // Alloca dinamicamente la memoria per la tabella hash
    globalHashtable.table = (OccurrenceNode **)malloc(size * sizeof(OccurrenceNode *));

    // Inizializza tutti gli elementi della tabella hash a NULL
    for (unsigned int i = 0; i < size; ++i)
    {
        globalHashtable.table[i] = NULL;
    }
}

OccurrenceNode *all_occurrences = NULL;
OccurrenceNode *local_occurrences;

// Funzione per deallocare la memoria della hashtable
void freeHashtable()
{
    // Dealloca le liste di trabocco e la tabella hash stessa
    for (unsigned int i = 0; i < globalHashtable.size; ++i)
    {
        OccurrenceNode *current = globalHashtable.table[i];
        while (current != NULL)
        {
            OccurrenceNode *temp = current;
            current = current->next;
            free(temp);
        }
    }

    free(globalHashtable.table);

    // Dealloca la memoria utilizzata per l'array di occorrenze
    free(all_occurrences);
    free(local_occurrences);
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

// Funzione per aggiornare il conteggio delle occorrenze di una parola nella hashtable
void updateWordCount(const char *word, int rank)
{
    // Calcola l'indice della hashtable utilizzando una funzione hash
    if (globalHashtable.size == 0)
    {
        // La hashtable non è stata inizializzata correttamente
        printf("Processo %d: Hashtable non inizializzata correttamente.\n", rank);
        return;
    }

    unsigned int hashIndex = hashFunction(word) % globalHashtable.size;
    // Cerca la parola nella lista di trabocco corrispondente
    OccurrenceNode *current = globalHashtable.table[hashIndex];
    while (current != NULL)
    {
        if (strcmp(current->word, word) == 0)
        {
            // La parola esiste nella hashtable, incrementa il conteggio
            current->count++;
            printf("Processo %d: Word %s found. Count updated to %d\n", rank, word, current->count);
            return; // Esci dalla funzione dopo l'aggiornamento
        }
        current = current->next;
    }

    // La parola non esiste, aggiungila alla lista di trabocco
    OccurrenceNode *newNode = (OccurrenceNode *)malloc(sizeof(OccurrenceNode));
    newNode->count = 1; // Prima occorrenza
    strncpy(newNode->word, word, MAX_WORD_LENGTH - 1);
    newNode->word[MAX_WORD_LENGTH - 1] = '\0'; // Assicura che la stringa sia terminata correttamente
    newNode->next = globalHashtable.table[hashIndex];

    // Aggiorna il puntatore alla testa della lista di trabocco
    globalHashtable.table[hashIndex] = newNode;

    // Aggiorna la hashtable globale con i conteggi locali
    for (int i = 0; i < local_total_occurrences; ++i)
    {

        updateWordCount(local_occurrences[i].word, rank);
    }
}

// Funzione per generare l'array dall'hash table
OccurrenceNode *generateOccurrencesArray(int *numOccurrences)
{
    // Inizializza il conteggio totale delle parole
    *numOccurrences = 0;

    // Alloca dinamicamente l'array delle occorrenze
    OccurrenceNode *occurrencesArray = (OccurrenceNode *)malloc(globalHashtable.size * sizeof(OccurrenceNode));

    // Riempie l'array con le occorrenze dalla hashtable
    int currentIndex = 0;
    for (unsigned int i = 0; i < globalHashtable.size; ++i)
    {
        OccurrenceNode *current = globalHashtable.table[i];
        while (current != NULL)
        {
            occurrencesArray[currentIndex].count = current->count;
            strncpy(occurrencesArray[currentIndex].word, current->word, MAX_WORD_LENGTH - 1);
            occurrencesArray[currentIndex].word[MAX_WORD_LENGTH - 1] = '\0'; // Assicura che la stringa sia terminata correttamente

            currentIndex++;
            current = current->next;
        }
    }

    *numOccurrences = currentIndex; // Aggiorna il conteggio totale

    return occurrencesArray;
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

    if (rank == MASTER_RANK)
    {

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
        start_byte = end_byte+1;
        end_byte = start_byte + avg_partition_size ;
    }
    else
    {
        // Caso di invio all'ultimo processo
        start_byte = end_byte + 1;
        end_byte = total_size + 1 ;
    }

    // Aggiungi byte aggiunti per non troncare un'eventuale parola in lettura
    while (end_byte < total_size +1 && all_data[end_byte] != ' ')
    {
        end_byte--;
    }

    // Calcola il numero effettivo di byte da inviare
    long byte_count = end_byte - start_byte + 1;

    // Invia start_byte, byte_count ai processi SLAVE
    printf("Processo %d: Invia a processo %d - start_byte: %ld, end_byte: %ld , byte_count: %ld\n", rank, i, start_byte, end_byte, byte_count);
    MPI_Isend(&start_byte, 1, MPI_LONG, i, 0, MPI_COMM_WORLD, &requests[i - 1]);
    MPI_Isend(&byte_count, 1, MPI_LONG, i, 0, MPI_COMM_WORLD, &requests[i - 1]);
    MPI_Isend(all_data + start_byte, byte_count, MPI_CHAR, i, 0, MPI_COMM_WORLD, &requests[i - 1]);
}


        // Attesa della completamento degli invii asincroni
        MPI_Waitall(num_processes - 1, requests, MPI_STATUSES_IGNORE);

        // Libera la memoria allocata per il buffer e le richieste
        free(all_data);
        free(requests);
    }
    else
    {
        // SLAVE
        long start_byte, byte_count;
        char *local_data;

        // Ricevi start_byte e byte_count dal processo MASTER
        MPI_Recv(&start_byte, 1, MPI_LONG, MASTER_RANK, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        MPI_Recv(&byte_count, 1, MPI_LONG, MASTER_RANK, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        // Allocazione di un buffer locale per contenere il chunk
        local_data = (char *)malloc(byte_count * sizeof(char));

        // Ricevi il chunk di dati dal processo MASTER
        MPI_Recv(local_data, byte_count, MPI_CHAR, MASTER_RANK, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        // Ora puoi processare il chunk locale come necessario

        // Esempio: Stampa il contenuto del chunk
        printf("Processo %d: Ricevuto chunk da start_byte %ld, byte_count %ld\n", rank, start_byte, byte_count);
        printf("Processo %d: Contenuto del chunk: %.*s\n", rank, (int)byte_count, local_data);

        // Libera la memoria allocata per il buffer locale
        free(local_data);
    }
    freeHashtable();
    MPI_Finalize();
    return 0;
}