#include <mpi.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define TRACKER_RANK 0
#define MAX_FILES 10
#define MAX_FILENAME 15
#define HASH_SIZE 32
#define MAX_CHUNKS 100

const int trackerTag = 0;
const int getTag = 5;
const int postTag = 10;
const int segmentTag = 15;

typedef struct {
  char nume[MAX_FILENAME];
  int nrTasks;
  int tasks[MAX_FILES];
} Owner;

typedef struct {
  int nrSegmente;
  char hash[MAX_CHUNKS][HASH_SIZE + 1];
} Segmente;

typedef struct {
  char nume[MAX_FILENAME];
  Segmente segmente;
} Fisier;

typedef struct {
  int nrFisiere, nrFisiereCerute, nrFisiereGata;
  Fisier fisiere[MAX_FILES];
  int nrOwners;
  Owner owners[MAX_FILES];
  char clients[MAX_FILES];
  int numtasks, rank;
} DB;

DB db;

void *download_thread_func() {
  int tracker[MAX_FILES] = {0};
  while (1) {
    if (db.nrFisiereGata == db.nrFisiereCerute) {
      break;
    }
    for (int index1 = db.nrFisiere; index1 < db.nrFisiere + db.nrFisiereCerute; index1++) {
      int flag = 0;
      MPI_Send(&flag, 1, MPI_INT, TRACKER_RANK, getTag, MPI_COMM_WORLD);
      MPI_Send(db.fisiere[index1].nume, MAX_FILENAME, MPI_CHAR, TRACKER_RANK, getTag, MPI_COMM_WORLD);
      int nrSegmente;
      MPI_Recv(&nrSegmente, 1, MPI_INT, TRACKER_RANK, getTag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
      int nrTasks;
      MPI_Recv(&nrTasks, 1, MPI_INT, TRACKER_RANK, getTag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
      int tasks[MAX_FILES];
      MPI_Recv(tasks, nrTasks, MPI_INT, TRACKER_RANK, getTag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
      char nume[256];
      snprintf(nume, sizeof(nume), "client%d_%s", db.rank, db.fisiere[index1].nume);
      FILE *out = fopen(nume, "w");
      db.nrFisiereGata++;
      for (int index2 = 0; index2 < nrSegmente; index2++) {
        int minChunks = 0x7F800000, urmatorulOwner;
        for (int i = 0; i < nrTasks; i++) {
          if (tracker[tasks[i]] < minChunks) {
            minChunks = tracker[tasks[i]];
            urmatorulOwner = tasks[i];
          }
        }
        int flag = 0;
        MPI_Send(&flag, 1, MPI_INT, urmatorulOwner, postTag, MPI_COMM_WORLD);
        MPI_Send(db.fisiere[index1].nume, MAX_FILENAME, MPI_CHAR, urmatorulOwner, postTag, MPI_COMM_WORLD);
        MPI_Send(&index2, 1, MPI_INT, urmatorulOwner, postTag, MPI_COMM_WORLD);
        tracker[urmatorulOwner]++;
        char hash[HASH_SIZE];
        MPI_Recv(hash, HASH_SIZE + 1, MPI_CHAR, urmatorulOwner, segmentTag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        tracker[urmatorulOwner]--;
        strcpy(db.fisiere[index1].segmente.hash[db.fisiere[index1].segmente.nrSegmente++], hash);
        fprintf(out, "%s\n", hash);
      }
      fclose(out);
    }
  }
  int flag = 1;
  MPI_Send(&flag, 1, MPI_INT, TRACKER_RANK, getTag, MPI_COMM_WORLD);
  return NULL;
}

void *upload_thread_func() {
  while (1) {
    MPI_Status status;
    int flag;
    MPI_Recv(&flag, 1, MPI_INT, MPI_ANY_SOURCE, postTag, MPI_COMM_WORLD, &status);
    switch (flag) {
    case 0: {
      char nume[MAX_FILENAME];
      MPI_Recv(nume, MAX_FILENAME, MPI_CHAR, status.MPI_SOURCE, postTag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
      int index = 0;
      for (int index1 = 0; index1 < db.nrFisiere; index1++) {
        if (strcmp(db.fisiere[index1].nume, nume) == 0) {
          index = index1;
          break;
        }
      }
      int segment;
      MPI_Recv(&segment, 1, MPI_INT, status.MPI_SOURCE, postTag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
      MPI_Send(&db.fisiere[index].segmente.hash[segment], HASH_SIZE + 1, MPI_CHAR, status.MPI_SOURCE, segmentTag,
               MPI_COMM_WORLD);
      break;
    }
    default: {
      return NULL;
    }
    }
  }
}

int gasire_owner(char *nume) {
  for (int index = 0; index < db.nrOwners; index++) {
    if (strcmp(nume, db.owners[index].nume) == 0) {
      return index;
    }
  }
  return db.nrOwners;
}

void primire() {
  for (int index1 = 1; index1 < db.numtasks; index1++) {
    int nrFisiere = 0;
    MPI_Recv(&nrFisiere, 1, MPI_INT, index1, trackerTag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    for (int index2 = 0; index2 < nrFisiere; index2++) {
      char nume[MAX_FILENAME];
      MPI_Recv(nume, MAX_FILENAME, MPI_CHAR, index1, trackerTag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
      strcpy(db.fisiere[db.nrFisiere].nume, nume);
      int nrSegmente = 0;
      MPI_Recv(&nrSegmente, 1, MPI_INT, index1, trackerTag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
      db.fisiere[db.nrFisiere].segmente.nrSegmente = nrSegmente;
      for (int index3 = 0; index3 < nrSegmente; index3++) {
        char hash[HASH_SIZE];
        MPI_Recv(hash, HASH_SIZE, MPI_CHAR, index1, trackerTag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        strcpy(db.fisiere[db.nrFisiere].segmente.hash[index3], hash);
        strcat(db.fisiere[db.nrFisiere].segmente.hash[index3], "\0");
      }
      int owner = gasire_owner(nume);
      if (owner == db.nrOwners) {
        db.owners[owner].nrTasks = 0;
        strcpy(db.owners[owner].nume, nume);
        db.nrOwners++;
      }
      db.owners[owner].tasks[db.owners[owner].nrTasks++] = index1;
      db.nrFisiere++;
    }
  }
}

int verificare_clients() {
  for (int index = 1; index < db.numtasks; index++) {
    if (db.clients[index] == 0) {
      return 0;
    }
  }
  return 1;
}

void tracker() {
  char msg[256] = "ACK";
  for (int index = 1; index < db.numtasks; index++) {
    MPI_Send(msg, 256, MPI_CHAR, index, trackerTag, MPI_COMM_WORLD);
  }
  while (1) {
    MPI_Status status;
    int flag;
    MPI_Recv(&flag, 1, MPI_INT, MPI_ANY_SOURCE, getTag, MPI_COMM_WORLD, &status);
    switch (flag) {
    case 0: {
      char nume[MAX_FILENAME];
      MPI_Recv(nume, MAX_FILENAME, MPI_CHAR, status.MPI_SOURCE, getTag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
      int index = 0;
      for (int index1 = 0; index1 < db.nrFisiere; index1++) {
        if (strcmp(db.fisiere[index1].nume, nume) == 0) {
          index = index1;
          break;
        }
      }
      MPI_Send(&db.fisiere[index].segmente.nrSegmente, 1, MPI_INT, status.MPI_SOURCE, getTag, MPI_COMM_WORLD);
      int owner = gasire_owner(nume);
      MPI_Send(&db.owners[owner].nrTasks, 1, MPI_INT, status.MPI_SOURCE, getTag, MPI_COMM_WORLD);
      MPI_Send(db.owners[owner].tasks, db.owners[owner].nrTasks, MPI_INT, status.MPI_SOURCE, getTag, MPI_COMM_WORLD);
      break;
    }
    default: {
      db.clients[status.MPI_SOURCE] = 1;
      if (verificare_clients() == 1) {
        int flag = 1;
        for (int index = 1; index < db.numtasks; index++) {
          MPI_Send(&flag, 1, MPI_INT, index, postTag, MPI_COMM_WORLD);
        }
        return;
      }
      break;
    }
    }
  }
}

void citire_si_trimitere() {
  char filename[MAX_FILENAME];
  sprintf(filename, "in%d.txt", db.rank);
  FILE *in = fopen(filename, "r");
  fscanf(in, "%d", &db.nrFisiere);
  MPI_Send(&db.nrFisiere, 1, MPI_INT, TRACKER_RANK, trackerTag, MPI_COMM_WORLD);
  for (int index1 = 0; index1 < db.nrFisiere; index1++) {
    fscanf(in, "%s", db.fisiere[index1].nume);
    strcat(db.fisiere[index1].nume, "\0");
    MPI_Send(db.fisiere[index1].nume, MAX_FILENAME, MPI_CHAR, TRACKER_RANK, trackerTag, MPI_COMM_WORLD);
    fscanf(in, "%d", &db.fisiere[index1].segmente.nrSegmente);
    MPI_Send(&db.fisiere[index1].segmente.nrSegmente, 1, MPI_INT, TRACKER_RANK, trackerTag, MPI_COMM_WORLD);
    for (int index2 = 0; index2 < db.fisiere[index1].segmente.nrSegmente; index2++) {
      fscanf(in, "%s", db.fisiere[index1].segmente.hash[index2]);
      strcat(db.fisiere[index1].segmente.hash[index2], "\0");
      MPI_Send(db.fisiere[index1].segmente.hash[index2], HASH_SIZE, MPI_CHAR, TRACKER_RANK, trackerTag, MPI_COMM_WORLD);
    }
  }
  fscanf(in, "%d", &db.nrFisiereCerute);
  for (int index = db.nrFisiere; index < db.nrFisiere + db.nrFisiereCerute; index++) {
    fscanf(in, "%s", db.fisiere[index].nume);
    db.fisiere[index].segmente.nrSegmente = 0;
  }
  fclose(in);
}

void peer() {
  char msg[256];
  MPI_Recv(msg, 256, MPI_CHAR, TRACKER_RANK, trackerTag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

  pthread_t download_thread;
  pthread_t upload_thread;
  void *status;
  int r;

  r = pthread_create(&download_thread, NULL, download_thread_func, NULL);
  if (r) {
    printf("Eroare la crearea thread-ului de download\n");
    exit(-1);
  }

  r = pthread_create(&upload_thread, NULL, upload_thread_func, NULL);
  if (r) {
    printf("Eroare la crearea thread-ului de upload\n");
    exit(-1);
  }

  r = pthread_join(download_thread, &status);
  if (r) {
    printf("Eroare la asteptarea thread-ului de download\n");
    exit(-1);
  }

  r = pthread_join(upload_thread, &status);
  if (r) {
    printf("Eroare la asteptarea thread-ului de upload\n");
    exit(-1);
  }
}

int main(int argc, char *argv[]) {
  int numtasks, rank;

  int provided;
  MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
  MPI_Comm_size(MPI_COMM_WORLD, &numtasks);
  MPI_Comm_rank(MPI_COMM_WORLD, &rank);

  db.numtasks = numtasks;
  db.rank = rank;

  if (rank == TRACKER_RANK) {
    primire();
    tracker();
  } else {
    citire_si_trimitere();
    peer();
  }

  MPI_Finalize();
}