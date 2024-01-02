#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>
#include <pthread.h>
#include <sys/stat.h>
#include <dirent.h>
#include <stddef.h>

#define MAX_WORD_LENGTH 100

const char *folderPath = "./InputFile";

struct File_inf
{
    int id;
    char name[64];
    char *path;
    size_t total_size;
    struct File_inf *next;
};

struct WordFreq
{
    char word[MAX_WORD_LENGTH];
    int frequency;
    struct WordFreq *next;
};

struct WordFreq *globalWordCounts;

int WordFreqCompare(struct WordFreq *a, struct WordFreq *b)
{
    return b->frequency - a->frequency;
}

struct File_inf *file_info_list;  // Variabile globale
struct WordFreq *localWordCounts; // Variabile globale
int wordCount = 0;                // Variabile globale

size_t calculateTotalSize(const char *folderPath)
{
    size_t total_size = 0;
    struct File_inf *file_info_list = NULL;
    struct dirent *entry;
    DIR *dp;

    dp = opendir(folderPath);

    if (dp == NULL)
    {
        perror("opendir");
        return 0;
    }

    int id = 0;
    while (entry = readdir(dp))
    {
        if (entry->d_type == DT_REG)
        {
            char filePath[PATH_MAX];
            snprintf(filePath, sizeof(filePath), "%s/%s", folderPath, entry->d_name);
            struct stat st;
            if (stat(filePath, &st) == 0)
            {
                struct File_inf *newFile = (struct File_inf *)malloc(sizeof(struct File_inf));
                newFile->id = id;
                strcpy(newFile->name, entry->d_name);
                newFile->path = strdup(filePath);
                newFile->total_size = st.st_size;
                newFile->next = file_info_list;
                file_info_list = newFile;
                id++;
            }
        }
    }

    closedir(dp);

    struct File_inf *current = file_info_list;
    while (current != NULL)
    {
        total_size += current->total_size;
        current = current->next;
    }

    // Assicurati di deallocare la memoria utilizzata da file_info_list quando hai finito con essa
    while (file_info_list != NULL)
    {
        struct File_inf *temp = file_info_list;
        file_info_list = file_info_list->next;
        free(temp->path);
        free(temp);
    }

    return total_size;
}

int compareWordFreq(const void *a, const void *b)
{
    return ((struct WordFreq *)b)->frequency - ((struct WordFreq *)a)->frequency;
}

// Funzione per scrivere gli istogrammi finali in un file di testo
void writeWordCountsToFile(struct WordFreq *wordCounts, const char *outputFilename)
{
    FILE *outputFile = fopen(outputFilename, "w");
    if (outputFile == NULL)
    {
        perror("fopen");
        return;
    }

    // Calcola il numero di istogrammi delle parole
    struct WordFreq *current = wordCounts;
    while (current != NULL)
    {
        wordCount++;
        current = current->next;
    }

    // Crea un array per contenere gli istogrammi delle parole
    struct WordFreq *wordArray = (struct WordFreq *)malloc(wordCount * sizeof(struct WordFreq));
    current = wordCounts;
    for (int i = 0; i < wordCount; i++)
    {
        strcpy(wordArray[i].word, current->word);
        wordArray[i].frequency = current->frequency;
        current = current->next;
    }

    // Ordina l'array degli istogrammi delle parole con QuickSort
    qsort(wordArray, wordCount, sizeof(struct WordFreq), compareWordFreq);

    // Scrivi gli istogrammi delle parole ordinati nel file di output
    for (int i = 0; i < wordCount; i++)
    {
        fprintf(outputFile, "%s: %d\n", wordArray[i].word, wordArray[i].frequency);
    }

    fclose(outputFile);
    free(wordArray);
}

void calculatePartitionSizes(int *partitionSizes, int numProcesses, size_t totalSize)
{
    size_t bytesPerProcess = totalSize / numProcesses;
    size_t extraBytes = totalSize % numProcesses;

    for (int i = 0; i < numProcesses; i++)
    {
        partitionSizes[i] = bytesPerProcess;
        if (i < extraBytes)
        {
            partitionSizes[i]++; // Distribuisci gli extraBytes equamente tra i primi processi
        }
    }
}

struct WordFreq *countWord(struct WordFreq *localWordCounts, const char *word)
{
    if (localWordCounts == NULL)
    {
        // La lista locale è vuota, crea un nuovo elemento
        struct WordFreq *newWord = (struct WordFreq *)malloc(sizeof(struct WordFreq));
        strcpy(newWord->word, word);
        newWord->frequency = 1;
        newWord->next = NULL;
        return newWord;
    }
    else
    {
        // Cerca se la parola è già presente nella lista locale
        struct WordFreq *current = localWordCounts;
        struct WordFreq *prev = NULL;

        while (current != NULL)
        {
            if (strcmp(current->word, word) == 0)
            {
                // La parola esiste già, incrementa il conteggio
                current->frequency++;
                return localWordCounts;
            }
            prev = current;
            current = current->next;
        }

        // La parola non esiste nella lista, crea un nuovo elemento
        struct WordFreq *newWord = (struct WordFreq *)malloc(sizeof(struct WordFreq));
        strcpy(newWord->word, word);
        newWord->frequency = 1;
        newWord->next = NULL;

        // Collega il nuovo elemento alla lista locale
        prev->next = newWord;
        return localWordCounts;
    }
}

void sendFileInfoList(struct File_inf *fileInfo, int dest)
{
    // Calcola il numero di elementi nella lista di informazioni sui file
    int numFiles = 0;
    struct File_inf *current = fileInfo;
    while (current != NULL)
    {
        numFiles++;
        current = current->next;
    }

    // Invia il numero di elementi al processo destinatario
    MPI_Send(&numFiles, 1, MPI_INT, dest, 0, MPI_COMM_WORLD);

    // Invia le informazioni sui file al processo destinatario
    current = fileInfo;
    while (current != NULL)
    {
        // Invia l'ID del file
        MPI_Send(&(current->id), 1, MPI_INT, dest, 0, MPI_COMM_WORLD);

        // Invia il nome del file
        MPI_Send(current->name, 64, MPI_CHAR, dest, 0, MPI_COMM_WORLD);

        // Invia il percorso del file
        int pathLength = strlen(current->path);
        MPI_Send(&pathLength, 1, MPI_INT, dest, 0, MPI_COMM_WORLD);
        MPI_Send(current->path, pathLength, MPI_CHAR, dest, 0, MPI_COMM_WORLD);

        // Invia la dimensione totale del file
        MPI_Send(&(current->total_size), 1, MPI_UNSIGNED_LONG, dest, 0, MPI_COMM_WORLD);

        current = current->next;
    }
}

struct File_inf *receiveFileInfoList(size_t partitionSize)
{
    int id;
    char name[64];
    char *path;
    size_t total_size;
    printf("1111111111111111111111");
    struct File_inf *receivedFileInfoList = NULL;
    struct File_inf *currentFileInfo = NULL;

    for (size_t i = 0; i < partitionSize; i++)
    {
        printf("222222222222222");
        // Ricevi l'ID del file
        MPI_Recv(&id, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        printf("33333333333333333");
        // Ricevi il nome del file
        MPI_Recv(name, 64, MPI_CHAR, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        printf("44444444444444444");
        // Ricevi la dimensione del percorso
        int pathLength;
        MPI_Recv(&pathLength, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        printf("5555555555555555");
        path = (char *)malloc((pathLength + 1) * sizeof(char));
        printf("6666666666666");
        MPI_Recv(path, pathLength, MPI_CHAR, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        path[pathLength] = '\0'; // Assicurati che il percorso sia una stringa C valida
        printf("777777777777777");
        // Ricevi la dimensione totale del file
        MPI_Recv(&total_size, 1, MPI_UNSIGNED_LONG, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        printf("888888888888");
        // Crea un nuovo elemento di informazioni sui file
        struct File_inf *newFileInfo = (struct File_inf *)malloc(sizeof(struct File_inf));
        newFileInfo->id = id;
        strcpy(newFileInfo->name, name);
        newFileInfo->path = path;
        newFileInfo->total_size = total_size;
        newFileInfo->next = NULL;

        // Aggiungi il nuovo elemento alla lista
        if (receivedFileInfoList == NULL)
        {
            receivedFileInfoList = newFileInfo;
            currentFileInfo = receivedFileInfoList;
        }
        else
        {
            currentFileInfo->next = newFileInfo;
            currentFileInfo = newFileInfo;
        }
    }
    return receivedFileInfoList;
}

void sendWordCounts(struct WordFreq *localWordCounts, int rank)
{
    // Conta il numero di parole nella hashtable locale
    int wordCount = 0;
    struct WordFreq *current = localWordCounts;
    while (current != NULL)
    {
        wordCount++;
        current = current->next;
    }

    // Invia il numero di parole al processo MASTER
    MPI_Send(&wordCount, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);

    // Invia le frequenze delle parole al processo MASTER
    current = localWordCounts;
    while (current != NULL)
    {
        // Invia la parola
        MPI_Send(current->word, MAX_WORD_LENGTH, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
        // Invia il conteggio
        MPI_Send(&current->frequency, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
        current = current->next;
    }
}

int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    int partitionSizes[size];
    char *data;

    if (rank == 0)
    {
        printf("SIAMO NEL MASTER - Start DEL MASTER!\n");
        fflush(stdout);
        // Passo 1: Il processo MASTER ottiene il percorso della cartella da cui leggere i file
        // Passo 2: Calcola la dimensione totale dei file nella cartella
        struct File_inf *file_info_list = NULL;
        const char *folderPath = "./InputFile";
        printf("MASTER - Folder Path: %s\n", folderPath);
        size_t totalSize = calculateTotalSize(folderPath);
        printf("MASTER - Total Size: %zu\n", totalSize);

        // Passo 3: Determina il numero di partizioni e la dimensione media
        size_t avgPartitionSize = totalSize / size;
        int remainderValue = totalSize % size;
        printf("MASTER - Avg Partition Size: %zu, Remainder: %d\n", avgPartitionSize, remainderValue);

        // Passo 4: Assegna partizioni ai processi SLAVE

        calculatePartitionSizes(partitionSizes, size, totalSize);
        printf("MASTER - Partition Sizes: ");
        for (int i = 0; i < size; i++)
        {
            printf("%d ", partitionSizes[i]);
        }
        printf("\n");
        printf("MASTER - Before sending partition info to SLAVE\n");
        // Invia le informazioni sulle partizioni ai processi SLAVE
        for (int i = 1; i < size; i++)
        {
            // Calcola l'indice iniziale per il processo SLAVE i
            size_t startIndex = 0;
            for (int j = 0; j < i; j++)
            {
                startIndex += partitionSizes[j];
            }

            // Calcola la dimensione della partizione per il processo SLAVE i
            size_t partitionSize = partitionSizes[i];

            // Crea un nuovo elenco di informazioni sul file per il processo SLAVE i
            struct File_inf *slaveFileInfo = NULL;
            struct File_inf *currentFileInfo = file_info_list;

            // Copia le informazioni sui file appropriate nel nuovo elenco
            for (size_t j = 0; j < partitionSize; j++)
            {
                if (currentFileInfo == NULL)
                {
                    break; // Se siamo alla fine dell'elenco dei file, interrompi
                }

                struct File_inf *newFileInfo = (struct File_inf *)malloc(sizeof(struct File_inf));
                newFileInfo->id = currentFileInfo->id;
                strcpy(newFileInfo->name, currentFileInfo->name);
                newFileInfo->path = strdup(currentFileInfo->path);
                newFileInfo->total_size = currentFileInfo->total_size;
                newFileInfo->next = slaveFileInfo;

                slaveFileInfo = newFileInfo;
                currentFileInfo = currentFileInfo->next;
            }
            printf("MASTER - Sending partitionSize to SLAVE %d: %zu\n", i, partitionSize);
            // Invia partitionSize e slaveFileInfo al processo SLAVE i
            MPI_Send(&partitionSize, 1, MPI_UNSIGNED_LONG, i, 0, MPI_COMM_WORLD);
            sendFileInfoList(slaveFileInfo, i);
            printf("MASTER - Sent partition info to SLAVE %d\n", i);
            MPI_Finalize();
        }

        MPI_Barrier(MPI_COMM_WORLD);
    }
    else
    {
        printf("SLAVE - Start (Rank %d)\n", rank);
        // Passo 4: I processi SLAVE ricevono le informazioni sulle partizioni dal processo MASTER
        size_t partitionSize;

        // Ricevi la dimensione della partizione dal processo MASTER
        MPI_Recv(&partitionSize, 1, MPI_UNSIGNED_LONG, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        printf("SLAVE - Received partitionSize: %zu (Rank %d)\n", partitionSize, rank);

        // Ricevi le informazioni sui file relative alla partizione
        struct File_inf *slaveFileInfo = receiveFileInfoList(partitionSize);
        printf("SLAVE - Received file info for partition (Rank %d)\n", rank);

        // Ora hai la dimensione della partizione e le informazioni sui file relative alla partizione

        // Passo 5: I processi SLAVE ricevono le partizioni assegnate dal MASTER e conteggiano le parole
        printf("SLAVE - Starting word counting (Rank %d)\n", rank);

        // Inizializza variabili per il conteggio delle parole
        struct WordFreq *localWordCounts = NULL;
        char *word = (char *)malloc(MAX_WORD_LENGTH * sizeof(char));
        int index = 0;

        // Calcola l'indice iniziale per il processo SLAVE corrente
        size_t startIndex = 0;
        for (int j = 0; j < rank - 1; j++)
        {
            startIndex += partitionSizes[j];
        }

        size_t dataLength = 0;

        // Leggi i dati dalla partizione e conteggia le parole
        // Apri il file associato alla partizione
        FILE *file = fopen(slaveFileInfo->path, "rb");

        if (file == NULL)
        {
            // Gestisci l'errore di apertura del file
            printf("Errore nell'apertura del file (Rank %d)\n", rank);
            // Puoi restituire un errore o gestire la situazione in base alle tue esigenze
        }
        else
        {
            // Vai alla posizione iniziale per la partizione
            fseek(file, startIndex, SEEK_SET);

            // Leggi i dati dalla partizione
            data = (char *)malloc(partitionSize + 1); // +1 per il terminatore nullo
            fread(data, sizeof(char), partitionSize, file);

            // Assicurati che i dati siano null-terminated
            data[partitionSize] = '\0';

            // Determina la lunghezza dei dati
            dataLength = partitionSize;

            // Chiudi il file
            fclose(file);

            // Ora hai letto i dati dalla partizione e conosci la loro lunghezza
        }

        for (size_t i = 0; i < dataLength; i++)
        {
            if (data[i] != ' ' && data[i] != '\n')
            {
                // Aggiungi il carattere corrente alla parola in costruzione
                word[index++] = data[i];
            }
            else
            {
                // Fine della parola, processala
                if (index > 0)
                {
                    word[index] = '\0';                                 // Termina la parola con il carattere nullo
                    localWordCounts = countWord(localWordCounts, word); // Funzione per conteggiare la parola
                }
                // Resetta l'indice per costruire la prossima parola
                index = 0;
            }
        }

        // Assicurati di processare l'ultima parola se il chunk termina con una parola
        if (index > 0)
        {
            word[index] = '\0';
            localWordCounts = countWord(localWordCounts, word);
        }

        // Libera il buffer di dati
        free(data);

        // Ora hai conteggiato le parole nella partizione

        // Passo 6: Invia le frequenze calcolate al processo MASTER
        printf("SLAVE - Sending word counts to MASTER (Rank %d)\n", rank);
        sendWordCounts(localWordCounts, rank);
    }

    if (rank == 0)
    {
        printf("MASTER - Before MPI_Recv from SLAVE");
        // Passo 7: Il processo MASTER riceve le frequenze dai processi SLAVE e le unisce per ottenere un unico istogramma globale
        // Definisci una struttura per ricevere le frequenze da un processo SLAVE
        struct SlaveWordCounts
        {
            int wordCount;
            struct WordFreq *wordList;
        };

        // Array per memorizzare i dati ricevuti da tutti i processi SLAVE
        struct SlaveWordCounts *allSlaveWordCounts = (struct SlaveWordCounts *)malloc((size - 1) * sizeof(struct SlaveWordCounts));

        // Ricevi le frequenze da tutti i processi SLAVE
        for (int source = 1; source < size; source++)
        {
            MPI_Status status;
            // Ricevi il numero di parole dal processo SLAVE
            MPI_Recv(&(allSlaveWordCounts[source - 1].wordCount), 1, MPI_INT, source, 0, MPI_COMM_WORLD, &status);

            // Alloca la memoria per le frequenze ricevute
            allSlaveWordCounts[source - 1].wordList = (struct WordFreq *)malloc(allSlaveWordCounts[source - 1].wordCount * sizeof(struct WordFreq));

            // Ricevi le parole e i conteggi dal processo SLAVE
            for (int i = 0; i < allSlaveWordCounts[source - 1].wordCount; i++)
            {
                MPI_Recv(allSlaveWordCounts[source - 1].wordList[i].word, MAX_WORD_LENGTH, MPI_CHAR, source, 0, MPI_COMM_WORLD, &status);
                MPI_Recv(&(allSlaveWordCounts[source - 1].wordList[i].frequency), 1, MPI_INT, source, 0, MPI_COMM_WORLD, &status);
            }

            // Combinare le frequenze ricevute con il globalWordCounts
            for (int i = 0; i < allSlaveWordCounts[source - 1].wordCount; i++)
            {
                globalWordCounts = countWord(globalWordCounts, allSlaveWordCounts[source - 1].wordList[i].word);
            }
        }
        writeWordCountsToFile(globalWordCounts, "output.csv");
        printf("MASTER - After writeWordCountsToFile");

        // Deallocazione della memoria allocata per le frequenze ricevute dai processi SLAVE
        for (int i = 0; i < size - 1; i++)
        {
            free(allSlaveWordCounts[i].wordList);
        }
        free(allSlaveWordCounts);
        printf("Before MPI_Finalize");
    }
    MPI_Finalize();
    return 0;
}
