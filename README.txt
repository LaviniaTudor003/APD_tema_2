Pentru aceasta tema, am avut de simulat protocolul BitTorrent de partajare peer-to-peer folosind MPI:
Astfel:
    - MPI - pentru comunicarea intre procese.
    - Pthreads - pentru paralelism la nivel de client.

Detalii de implementare:

- Tracker:
           - primeste informatii de la clienti despre fisierele disponibile si creeaza swarm urile pentru fiecare fisier
           - primeste hash-urile segmentelor si le transmite clientilor, iar clientii cer segmente specificand indexul segmentului
           obs: transferul efectiv al segmentelor are loc direct intre clienti
           - asigura terminarea procesului si inchiderea clientilor.

- Prin threadul de upload, segmentele descarcate sunt disponibile imediat altor peers.

- Fiecare client isi declara ce fisiere are disponibile si ce fisiere vrea

- trackerul organizeaza lista de peers pentru fiecare fisier

- Clientii descarca segmente ale fisierelor cerute de la alti peers/seeds
    - trimit cereri MPI si primesc ACK la fiecare segment descarcat

- Dupa descarcare, salvez local segmentele si le pun la dispozitia altor clienti
- Dupa ce toti clientii termina descarcarile, trackerul si clientii isi incheie activitatea.
 
Eficienta este asigurata de download_thread_func:
-  folosesc un workload pentru a alege eficient peer ul.
