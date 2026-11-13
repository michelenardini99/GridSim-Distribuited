# Guida e Script per la Presentazione — GridSim-Distributed

Questa guida accompagna le 13 slide della presentazione ([index.html](file:///Users/matteo/Documents/Projects/unibo/ds/GridSim-Distribuited/docs/presentation/index.html)).
Per ogni slide trovi:
1. **Elenco puntato rapido** (da tenere sott'occhio se preferite parlare a braccio).
2. **Traccia discorsiva (Script)** (un discorso fluido già calibrato sui tempi).

> **Gestione del tempo (Target: 12–15 minuti totali)**
> - **Slide 1–3 (Contesto & Motivazioni)**: ~2.5 minuti
> - **Slide 4–5 (Architettura, Infrastruttura & Attori)**: ~2.5 minuti
> - **Slide 6–8 (Tick Scatter/Gather, Kafka Data Plane & Redis Event Sourcing)**: ~3 minuti
> - **Slide 9–12 (Teorema CAP, Fault Tolerance, Chaos Testing & Limiti)**: ~3.5 minuti
> - **Slide 13 (Live Demo)**: ~2.5 – 3.5 minuti

---

## Slide 1 — Titolo & Introduzione *(~45 sec)*

### Punti chiave
- Presentazione di **GridSim-Distributed** (Matteo Bambini, Michele Nardini).
- Simulatore di micro-reti elettriche riprogettato come **sistema distribuito, event-sourced e fault-tolerant**.
- Stack principale: **Scala 3**, **Apache Pekko** (Cluster, Sharding, Persistence), **Apache Kafka**, **Redis**, **Protobuf** e **ScalaFX**.

### Traccia discorsiva
> "Buongiorno, siamo Matteo Bambini e Michele Nardini e oggi vi presentiamo **GridSim-Distributed**. Il progetto nasce con l'obiettivo di simulare una micro-rete elettrica — composta da abitazioni, pannelli fotovoltaici, sistemi di accumulo e cavi — non come un programma monolitico su singolo processo, ma come un vero e proprio **sistema distribuito reattivo e tollerante ai guasti**. Nella presentazione daremo prima un rapido contesto sul dominio applicativo, per poi concentrarci sulle scelte architetturali di sistemi distribuiti: dal modello ad attori con Cluster Sharding, alla separazione tra Control Plane e Data Plane, fino all'Event Sourcing su Redis, alle scelte fatte rispetto al **Teorema CAP** e alla gestione dei fallimenti, concludendo con una **Live Demo**."

---

## Slide 2 — Il Dominio: Simulazione di una Micro-Grid Energetica *(~1 min)*

### Punti chiave
- **Modello a grafo orientato (`GridGraph`)**:
  - **Nodi (`GridEntity`)**: `House` (consumo + eventuali pannelli/batterie interne), `SolarPanel` (produzione da irraggiamento), `Storage` (batteria con limiti di capacità, potenza e `minSoC`), ed `ExternalGrid` (sorgente/pozzo infinito).
  - **Archi (`Cable`)**: linee elettriche con una capacità massima di trasporto.
- **Evoluzione a passi discreti (`tick`)**:
  1. Aggiornamento dell'`Environment` (ora simulata, temperatura, irraggiamento).
  2. Ogni entità risolve il proprio bilancio locale producendo un `Flow[Energy]` netto (*Surplus*, *Deficit* o *Balanced*).
  3. Un solutore globale (`KirchhoffPowerFlowSolver`) calcola i flussi sui cavi e compensa lo sbilancio con la `ExternalGrid`.
- **Architettura interna**: *Functional Core, Imperative Shell* (stato 100% immutabile, funzioni pure, scenari definiti con uno **Scala DSL** interno con validazione accumulante `ValidatedNec`).

### Traccia discorsiva
> "Partiamo brevemente dal dominio: cosa simula GridSim? Modella una micro-grid come un grafo orientato in cui i nodi sono le entità energetiche — case, impianti solari, batterie e la rete elettrica esterna — e gli archi sono i cavi con una capacità massima.
> La simulazione avanza a passi discreti chiamati **tick**: a ogni tick aggiorniamo l'ambiente meteorologico, ogni entità calcola in autonomia il proprio bilancio energetico locale restituendo un surplus o un deficit netto, e infine un solutore basato sulle leggi di Kirchhoff calcola la distribuzione dei flussi sui cavi, evidenziando eventuali sovraccarichi.
> A livello di design del codice abbiamo adottato il pattern **Functional Core, Imperative Shell**: tutta la fisica del dominio e lo stato sono completamente immutabili e privi di side-effect, mentre tutta la complessità distribuita è confinata negli attori."

---

## Slide 3 — Perché un Sistema Distribuito? *(~1 min)*

### Punti chiave
- **1. Resource Sharing & Separation of Concerns**:
  - Motore di simulazione (CPU/state-heavy, esecuzione continua) disaccoppiato dalle GUI (interaction-heavy).
  - Un cluster backend condiviso può eseguire più simulazioni; più client GUI remoti possono attaccarsi, osservare e staccarsi liberamente.
- **2. Parallelismo Computazionale & Scalabilità Orizzontale**:
  - Il calcolo locale delle entità dentro un tick è intrinsecamente indipendente $\rightarrow$ distribuibile su più nodi fisici tramite **Cluster Sharding**.
- **3. Resilienza & Dependability (Fault Tolerance)**:
  - Nelle simulazioni long-running il crash di un processo non deve distruggere lo stato: persistenza esternalizzata su Redis e migrazione trasparente degli shard sui nodi superstiti.

### Traccia discorsiva
> "Perché distribuire questo sistema anziché realizzare una classica applicazione desktop monolitica? Ci sono tre motivazioni architetturali concrete:
> La prima è il **Resource Sharing e la separazione dei compiti**: il motore di simulazione è un processo continuo e pesante che deve girare in modo affidabile su un cluster, mentre le GUI sono interattive e devono restare fluide; disaccoppiandoli, più client possono osservare contemporaneamente la stessa simulazione senza rubare cicli di CPU al calcolo matematico.
> La seconda è la **scalabilità orizzontale**: poiché dentro ciascun tick ogni casa, batteria o pannello calcola il proprio bilancio in totale autonomia, decomponiamo la rete in attori distribuiti con il **Cluster Sharding**, sfruttando le risorse di più nodi fisici in parallelo.
> La terza è la **Fault Tolerance**: una simulazione su vasta scala non può perdere ore di calcolo se un container cade. Con lo stato esternalizzato su Redis, se un nodo del cluster fallisce, gli attori vengono istantaneamente ricreati sui nodi sani e riprendono dall'ultimo tick consolidato senza alcuna perdita di dati."

---

## Slide 4 — Vista Infrastrutturale: Control Plane vs Data Plane *(~1.5 min)*

### Punti chiave
- **Architettura generale a tre livelli**:
  - **Client GUI (ScalaFX)**: leggeri e *stateless*, osservano una o più simulazioni senza ospitare logica di calcolo.
  - **Cluster Backend (3 repliche `GridSimAgentSystem`, Apache Pekko)**: comunicano via **Pekko Artery TCP** e si scoprono dinamicamente tramite DNS Docker (`CLUSTER_SERVICE_NAME`, nessun IP statico).
  - **Livello di Messaggistica & Persistenza esterno**: broker **Kafka** e store **Redis**.
- **Separazione netta tra due piani di comunicazione**:
  - **Control Plane (HTTP REST + JSON)**: sincrono, bassa frequenza (creazione, `start`, `pause`, `step`, `stop`, cambio velocità/tick). Passa da un gateway **Caddy** in round-robin verso i nodi backend.
  - **Data Plane (Apache Kafka + Protobuf)**: asincrono pub/sub, alta frequenza (telemetria a ogni tick). Il backend produce, $N$ GUI consumano senza che il backend debba tenere traccia degli osservatori.
- **Shared Persistence Store**: un'istanza **Redis** esterna condivisa da tutti i nodi del cluster per il salvataggio di eventi e snapshot (nessuno stato locale su disco).

### Traccia discorsiva
> "Prima di entrare nel dettaglio dei singoli componenti, diamo uno sguardo d'insieme all'architettura del sistema. Abbiamo strutturato GridSim su tre livelli disaccoppiati: in alto i client GUI ScalaFX, leggeri e completamente *stateless*; al centro il cuore computazionale, ovvero un **cluster Apache Pekko** composto da 3 nodi backend che comunicano via **Artery TCP** e si scoprono dinamicamente tramite il DNS interno di Docker; alla base, i servizi infrastrutturali esterni per streaming e persistenza.
> Per far dialogare client e cluster senza creare colli di bottiglia, abbiamo separato l'interazione su due piani distinti in base al profilo di traffico:
> Il **Control Plane** usa **HTTP REST con JSON** per i comandi di ciclo di vita a bassa frequenza, come avvio, pausa, step o cambio di velocità. I client parlano con un unico indirizzo esposto dal reverse proxy **Caddy**, che bilancia in round-robin sulle repliche del backend ed esclude automaticamente i nodi che non rispondono.
> Il **Data Plane**, invece, trasporta il flusso continuo di telemetria ad alta frequenza usando **Apache Kafka** e messaggi binari **Protocol Buffers**: è un canale pub/sub puro che permette a qualunque numero di GUI di iscriversi senza che il backend sappia chi sta ascoltando.
> Infine, per la **Shared Persistence**, nessun nodo del cluster salva dati sul proprio disco locale: tutti i nodi condividono un unico store **Redis** esterno per eventi e snapshot, rendendo ogni nodo sostituibile in caso di guasto."

---

## Slide 5 — Il Backend Pekko: Singleton, Sharding & Location Transparency *(~1 min)*

### Punti chiave
- **Tre tipologie di attori persistenti**:
  1. `SimulationRegistryActor` (**Cluster Singleton**): unica istanza nel cluster (sul nodo più anziano), mantiene la mappa delle simulazioni attive; se il nodo cade, viene ri-eletto su un altro nodo.
  2. `SimulationActor` (**Cluster Sharding** con `rememberEntities = true`): uno per simulazione (`simulationId`), gestisce la macchina a stati (`Idle`, `Running`, `Paused`, `Stopped`) e orchestra i tick.
  3. `EntityActor` (**Cluster Sharding**): uno per ogni nodo della micro-grid (`simulationId-localId`), possiede in esclusiva lo stato dinamico dell'entità.
- **Location & Migration Transparency**: né le rotte HTTP né `SimulationActor` conoscono l'allocazione fisica degli attori sui nodi; l'interazione avviene unicamente tramite `EntityRef`.

### Traccia discorsiva
> "Entrando nel backend Pekko, l'architettura si basa su tre attori persistenti:
> Il primo è il `SimulationRegistryActor`, istanziato come **Cluster Singleton**: ne esiste uno solo in tutto il cluster e tiene traccia delle simulazioni attive.
> Il secondo è il `SimulationActor`, distribuito tramite **Cluster Sharding** con l'opzione `remember-entities` attiva: modella la macchina a stati della singola simulazione e funge da orchestratore.
> Il terzo è l'`EntityActor`, anch'esso sharded tramite un ID logico composto da `simulationId` e ID locale dell'entità: ogni casa, pannello o batteria della rete è un attore indipendente.
> Grazie al Cluster Sharding otteniamo **Location Transparency** completa: quando una rotta HTTP o un `SimulationActor` invia un messaggio a un'entità tramite `EntityRef`, non sa su quale dei 3 nodi fisici si trovi l'attore."

---

## Slide 6 — Anatomia di un Tick: Il Protocollo Scatter / Gather *(~1.2 min)*

### Punti chiave
- **Tassonomia Spazio-Temporale, Logical Clocks & Conit**:
  - A livello fisico il cluster opera in *Distributed Concurrent Computing* ($S \neq S', T \neq T'$), ma `SimulationActor` impone un **Contesto Temporale Logico Condiviso $T$** su entità distribuite spazialmente ($S_i \neq S_j$).
  - `tick: Long` funge da **Scalar Logical Clock** per l'ordinamento causale dei round.
  - Ogni `GridEntityState` dentro `EntityActor` è un **Conit indipendente**: evolve senza lock globali intra-tick, sincronizzandosi solo al confine del tick.
- **Fasi di un singolo tick**:
  1. Scatta `TickTimer` su `SimulationActor` $\rightarrow$ avanza l'`Environment` di $\Delta t$.
  2. **Scatter**: `SimulationActor` invia in parallelo `ask(Evolve)` a tutti gli `EntityActor` della rete (con timeout `tickTimeout`).
  3. **Evoluzione parallela**: ogni `EntityActor` calcola il nuovo stato e il `Flow`, persiste l'evento `Evolved` su Redis, pubblica la propria telemetria su Kafka e risponde `EntityEvolved`.
  4. **Gather non bloccante (Latency Hiding)**: `Future.sequence` + `context.pipeToSelf` raccoglie tutte le risposte senza bloccare la mailbox dell'attore coordinatore.
  5. **Risoluzione globale**: `SimulationActor` persiste `TickAdvanced`, esegue `KirchhoffPowerFlowSolver.solve()`, pubblica i carichi dei cavi su Kafka e schedula il `TickTimer` successivo.

### Traccia discorsiva
> "Vediamo ora cosa succede durante un singolo tick di simulazione, collegandoci alla **tassonomia spazio-temporale del corso**. Sebbene i nodi del cluster operino in *Distributed Concurrent Computing*, ad ogni tick il `SimulationActor` impone un **contesto temporale logico condiviso $T$** sopra `EntityActor` distribuiti in contesti spaziali distinti ($S_i \neq S_j$), dove il numero di `tick` funge da **Scalar Logical Clock**.
> Isolando ogni entità come un **Conit indipendente**, nessuna entità deve sincronizzarsi con le altre durante il calcolo: lo scambio avviene solo al confine del tick tramite **Scatter/Gather**.
> Qui applichiamo inoltre il **Latency Hiding asincrono** combinando `Future.sequence` con `pipeToSelf`: il thread di `SimulationActor` non si blocca mai in attesa della rete, lasciando la mailbox libera di ricevere `Pause` o `Stop` mentre le $N$ entità evolvono in parallelo sui vari nodi."

---

## Slide 7 — Data Plane Kafka & Sincronizzazione Client *(~1 min)*

### Punti chiave
- **3 Topic Kafka per ogni simulazione**:
  1. `grid.entities.<simId>` (Protobuf `EntityTelemetryMessage`, partizionato per `entityId`).
  2. `grid.ticks.<simId>` (Protobuf `GridTickMessage`, ambiente e carichi sui cavi).
  3. `grid.control.<simId>` (stato lifecycle, velocità, $\Delta t$ e flag `degraded`).
- **Come la GUI Stateless ricostruisce uno stato coerente (`RemoteSimulationBuilder`)**:
  - Ogni GUI usa un `group.id` casuale con `auto.offset.reset = earliest` per fare replay della storia dei tick all'apertura.
  - **Barriera di allineamento lato client**: poiché i messaggi su `grid.entities` e `grid.ticks` viaggiano su topic separati, il client li accoda in `entityQueues` e `pendingTicks` ed emette lo snapshot alla UI solo quando **tutte le entità note hanno raggiunto il tick $T$**.
- **Isolamento**: errori su Kafka non bloccano né fanno fallire il tick sul backend.

### Traccia discorsiva
> "Sul Data Plane Kafka usiamo tre topic per ogni simulazione: uno per gli stati delle singole entità, uno per i dati globali del tick e dei cavi, e uno di controllo. I primi due sono codificati in **Protobuf** per ridurre al minimo l'overhead di rete ad alta frequenza.
> Un aspetto interessante di sistemi distribuiti riguarda il client GUI (`RemoteSimulationBuilder`): dato che gli `EntityActor` pubblicano su `grid.entities` in modo indipendente da quando `SimulationActor` pubblica su `grid.ticks`, i messaggi arrivano al consumer Kafka in ordine sparso tra i topic.
> Per evitare di disegnare sulla GUI uno stato parziale o incoerente, il client implementa una **barriera di riallineamento per tick**: bufferizza i messaggi in code per entità ed emette il nuovo `SimulationSnapshot` reattivo solo quando tutte le entità della rete hanno raggiunto quel numero di tick."

---

## Slide 8 — Event Sourcing, Snapshotting & Single-Writer su Redis *(~1.2 min)*

### Punti chiave
- **Event Sourcing su Shared Storage (Redis)**:
  - Superamento del modello CRUD: non sovrascriviamo lo stato mutabile, ma salviamo in modalità *append-only* la sequenza cronologica di eventi immutabili (`Initialized`, `Evolved`, `TickAdvanced`).
  - **PWD Assumption & Replay**: grazie alla purezza del *Functional Core*, rigiocando gli eventi salvati qualsiasi replica del cluster ricostruisce l'esatto stato dell'attore pre-crash.
  - **Pessimistic Logging contro l'Output Commit Problem**: l'evento viene committato stabilmente su Redis prima di rispondere al coordinatore o emettere telemetria verso Kafka.
- **Logging + Checkpointing & Riduzione del MTTR**:
  - `EntityActor`: **snapshot ogni 100 eventi** (`keepNSnapshots = 2`) con potatura degli eventi obsoleti per mantenere costante e prevedibile il tempo di riavvio.
  - Abbattimento del **MTTR** (*Mean Time To Repair*) per massimizzare la disponibilità: $\text{Availability} = \frac{\text{MTTF}}{\text{MTTF} + \text{MTTR}}$.
- **Single-Writer Invariant (Consistenza senza Lock)**:
  - Non servono lock distribuiti o transazioni complesse (2PC) su Redis: Cluster Sharding garantisce a monte che esista un solo attore scrittore attivo per ciascuna entità.

### Traccia discorsiva
> "Come gestiamo la persistenza dello stato distribuito? Adottiamo il pattern dell'**Event Sourcing** su Redis.
> Invece di fare aggiornamenti distruttivi dello stato come in un classico database relazionale, ogni attore persiste in modo immutabile la sequenza degli eventi che ne determinano l'evoluzione. Poiché la nostra fisica di calcolo è pura e deterministica, rispettiamo in pieno la **Piecewise Deterministic Assumption**: in caso di guasto, un attore ricreato su un nodo superstite rilegge la sequenza degli eventi e converge esattamente allo stato pre-crash.
> Per evitare che client o altri attori osservino stati non ancora consolidati, usiamo il **Pessimistic Logging**: l'evento viene scritto stabilmente su Redis prima di confermare il tick o emettere telemetria su Kafka, risolvendo alla radice l'*Output Commit Problem*.
> Inoltre, per evitare che con l'avanzare delle ore il tempo di replay diventi un collo di bottiglia, combiniamo **Logging e Checkpointing**: ogni 100 eventi l'entità salva uno snapshot completo e pota gli eventi storici superflui, abbattendo il **MTTR** e massimizzando l'**Availability**. Il tutto senza lock su Redis grazie al **Single-Writer Invariant** imposto dal Cluster Sharding."

---

## Slide 9 — Il Teorema CAP in GridSim: Scelte Progettuali *(~1.5 min)*

### Punti chiave
- **Write Path: CP — Safety over Liveness & Monotonic Writes**:
  - In presenza di una partizione ($P$), privilegiamo la **Safety** (nessuno stato/journal corrotto) rispetto alla **Liveness** sul lato isolato, preservando il *Single-Writer Invariant* su Redis.
  - Il contatore `max_seq` su Redis garantisce **Monotonic Writes** per ciascuna entità.
- **Mitigazione pratica FLP & Quorum $N \ge 2f + 1$**:
  - **Split Brain Resolver (`keep-majority`, `stable-after = 10s`)** su $N = 3$ nodi ($N \ge 2f + 1$, tollera $f = 1$): il lato maggioritario (2/3) sopravvive e ri-alloca gli shard; il lato minoritario (1/3) fa *self-downing* sacrificando la *Liveness*. Su partizione simmetrica (1-1-1) l'intero cluster si ferma a tutela della *Safety*.
- **Read Path: Modello BASE & Monotonic Reads**:
  - Il client adotta il modello **BASE**: non ha memoria persistente ma solo **Soft State** volatile in memoria, rigenerabile a caldo rileggendo Kafka.
  - Garantisce **Monotonic Reads**: l'interfaccia avanza solo su tick strettamente crescenti, impedendo all'utente di osservare la simulazione tornare indietro nel tempo.
  - Comportamento **Fail-Safe**: in caso di guasti del cluster o perdita di connessione, passa esplicitamente a `PAUSED` o `DEGRADED` invece di spacciare dati vecchi per attuali.

### Traccia discorsiva
> "Colleghiamo ora queste scelte al **Teorema CAP** e al consenso. In presenza di una partizione di rete ($P$), sul **Write Path** del backend abbiamo scelto **CP**, ovvero **Safety over Liveness**: non possiamo tollerare *split-brain* con due attori che scrivono con lo stesso `sequenceNr` su Redis, dove peraltro `max_seq` garantisce **Monotonic Writes**.
> Per imporre questa scelta usiamo lo **Split Brain Resolver `keep-majority`**: con 3 nodi ($N = 3 \ge 2f + 1$, quindi tolleriamo $f = 1$ nodo isolato), solo il lato con il quorum di maggioranza (2 su 3) sopravvive e ri-spawna gli attori, mentre la minoranza sacrifica la *Liveness* facendo *self-downing*.
> Sul **Read Path dei client**, invece, adottiamo il modello **BASE**: la GUI mantiene solo un **Soft State** volatile, completamente rigenerabile rileggendo i flussi da Kafka. Il client impone la proprietà di **Monotonic Reads**, scartando eventuali messaggi fuori ordine per evitare che l'utente veda la simulazione regredire nel tempo, e adotta un comportamento *fail-safe* evidenziando subito lo stato `PAUSED` o `DEGRADED` in caso di perdita di contatto con il cluster."

---

## Slide 10 — Fault Tolerance Intra-Tick: Timeout, Backoff & Idempotenza *(~1 min)*

### Punti chiave
- **Tassonomia Dependability**:
  1. **Fault Detection (`tickTimeout = 1s`)**: rileva **Omission / Timing failures** e **Crash failures** durante il gather $\rightarrow$ `TickFailed` $\rightarrow$ il tick non avanza.
  2. **Physical Redundancy vs Time Redundancy + Fail-Safe**: alle 3 repliche fisiche affianchiamo la **Time Redundancy** (retry con backoff esponenziale $1\text{ s} \to 30\text{ s}$ e `restartWithBackoff`). Dopo 3 fallimenti consecutivi il sistema attua un comportamento **Fail-Safe**, transendo in stato esplicito `DEGRADED` (e `PAUSED` lato GUI se cade l'API HTTP).
  3. **Idempotenza (`lastTick`)**: al retry del tick $T$, le entità che avevano già risposto controllano `tick <= lastTick` e **restituiscono `lastFlow` in cache senza ricalcolare né persistere un secondo evento**.

### Traccia discorsiva
> "Vediamo ora la **Fault Tolerance intra-tick** secondo la tassonomia della **Dependability**. Il `tickTimeout` rileva sia *Omission/Timing failures* che *Crash failures*: se anche una sola entità su 5 non risponde per il tick 10, `SimulationActor` riceve `TickFailed`.
> Qui combiniamo alla *Physical Redundancy* delle 3 repliche una **Time Redundancy** tramite **Exponential Backoff**; se i fallimenti persistono per 3 volte, il sistema assume un comportamento **Fail-Safe**, segnalando lo stato `DEGRADED` alle GUI.
> Al retry del tick 10, le 4 entità che avevano già evoluto e persistito su Redis usano il controllo `tick <= lastTick` per rispondere in modo **idempotente** con il `lastFlow` salvato, mentre solo la quinta entità — migrata su un nodo sano — esegue il calcolo mancante."

---

## Slide 11 — Validazione: Dal Unit Testing al Chaos Testing *(~1 min)*

### Punti chiave
- **3 livelli di validazione automatica**:
  1. **Unit & Actor TestKit (51 suite)**: dominio, solutori di Kirchhoff, DSL, codec Protobuf e test con `PersistenceTestKit` per verificare retry parziali, idempotenza e recovery degli attori.
  2. **Boundary Integration Tests (con container Docker)**: test end-to-end su Kafka/GUI e crash test quando l'API HTTP o il broker Kafka sono irraggiungibili (`HttpApiUnavailable`, `KafkaUnavailable`).
  3. **Chaos Testing sul Cluster (`ClusterResilienceIntegrationSpec`)**: avvia un vero cluster a 3 nodi con Redis e uccide bruscamente i nodi (`ActorSystem` termination) verificando:
     - Ri-elezione del `SimulationRegistryActor` singleton e recupero delle simulazioni registrate.
     - Failover di un `EntityActor` su un altro nodo, replay da Redis e risposta idempotente al retry.
     - Auto-spegnimento della minoranza (1 su 3) da parte dello Split Brain Resolver.

### Traccia discorsiva
> "Un punto a cui abbiamo tenuto molto è verificare queste proprietà non solo sulla carta, ma tramite test automatici di **fault-injection e chaos testing**.
> Oltre alle 51 suite di test unitari e ai test con `PersistenceTestKit` che simulano fallimenti parziali dei tick, abbiamo realizzato `ClusterResilienceIntegrationSpec`. Questo test avvia un vero cluster Pekko a 3 nodi appoggiato a Redis e uccide bruscamente i nodi mentre il sistema è attivo, verificando automaticamente tre cose: che il Cluster Singleton venga ri-eletto su un nodo superstite senza perdere le simulazioni registrate; che un `EntityActor` ospitato sul nodo ucciso venga ricreato su un nodo vivo, faccia replay da Redis e risponda in modo idempotente al retry del tick; e infine che, uccidendo 2 nodi su 3, il nodo rimasto in minoranza venga effettivamente abbattuto dallo Split Brain Resolver."

---

## Slide 12 — I 5 Goal & Degree of Transparency *(~45 sec)*

### Punti chiave
- **Mappa Teoria (Tanenbaum + Omicini) $\rightarrow$ Implementazione GridSim**:
  1. **Resource Availability**: motore backend sul cluster condiviso tra più GUI leggere.
  2. **Transparency & *Degree of Transparency***:
     - *Access, Location, Migration, Relocation* $\rightarrow$ `EntityRef` (Sharding) + Singleton.
     - *Replication* $\rightarrow$ Reverse proxy Caddy su `:8080`.
     - *Concurrency* $\rightarrow$ mailbox single-threaded + *Single-Writer* su Redis.
     - *Failure Transparency (Parziale intenzionale)* $\rightarrow$ maschera crash transitori intra-cluster, ma **espone** guasti persistenti (`DEGRADED` / `PAUSED`) all'utente.
  3. **Openness**: confini esterni *language-agnostic* (HTTP/REST JSON + Protobuf IDL), messaggi interni JVM-coupled.
  4. **Scalability (Neuman)**: scala in **Size** (sharding + *Latency Hiding*), su singolo dominio amministrativo (Docker Compose).
  5. **Situatedness (5° Goal)**: **Non applicabile** al sistema attuale (`Environment` è un input simulato, non un contesto fisico reale).

### Traccia discorsiva
> "Prima di chiudere con i limiti e la demo, valutiamo GridSim rispetto ai **5 Goal dei Sistemi Distribuiti del corso**.
> Il sistema soddisfa **Resource Availability**, **Openness** ai confini tramite HTTP/JSON e Protobuf, e **Scalability** sulla dimensione *Size* via sharding e *Latency Hiding*.
> Sul fronte della **Transparency**, regoliamo consapevolmente il **Degree of Transparency**: garantiamo *Location, Migration, Relocation, Replication* e *Concurrency Transparency*, ma manteniamo la **Failure Transparency intenzionalmente parziale**, esponendo `DEGRADED` e `PAUSED` all'utente su guasti persistenti perché la trasparenza totale non è sempre desiderabile.
> Infine, con onestà architetturale, il 5° goal di Omicini — la **Situatedness** — **non si applica** a questo simulatore, poiché l'`Environment` è un parametro simulato deterministico e non un ambiente fisico reale a cui il cluster si adatta."

---

## Slide 13 — Limiti Attuali e Sviluppi Futuri *(~40 sec)*

### Punti chiave
- **Limiti attuali (onestà ingegneristica)**:
  - **Redis e Kafka come SPOF**: il cluster backend è replicato su 3 nodi, ma nel `compose.yml` attuale Redis e Kafka sono istanze singole non replicate.
  - **Serializzazione Java intra-cluster/Redis**: a differenza di Kafka (che usa Protobuf), i messaggi interni Pekko e gli eventi su Redis usano la serializzazione Java.
  - **Health-check passivo su Caddy**: basato sui fallimenti HTTP anziché su probe attive di cluster readiness.
- **Sviluppi futuri**:
  - Redis Sentinel/Cluster e Kafka multi-broker replicato.
  - Estensione di Protobuf anche a Pekko Artery e Redis Journal.
  - Probe attive con `pekko-management` e autenticazione JWT / SASL-TLS.

### Traccia discorsiva
> "Prima di passare alla demo, facciamo una breve analisi critica dei limiti attuali.
> Aver esternalizzato lo stato su Redis rende i nodi Pekko completamente sostituibili, ma nel nostro setup Docker Compose sia Redis che Kafka girano come container singoli: abbiamo quindi eliminato il Single Point of Failure dal livello di calcolo, spostandolo però sullo storage e sul broker. In un ambiente di produzione il passo naturale sarebbe passare a Redis Sentinel o Redis Cluster e a un cluster Kafka multi-broker.
> Inoltre, mentre verso l'esterno usiamo Protobuf, per i messaggi interni al cluster e per Redis abbiamo usato la serializzazione Java standard per rapidità di sviluppo; unificare tutto sotto Protobuf permetterebbe di disaccoppiare anche il journal dalle classi JVM. Infine, si potrebbero introdurre health-check attivi tramite `pekko-management`."

---

## Slide 14 — Conclusioni, Q&A e Live Demo *(~1.5 min)*

### Punti chiave
- **Chiusura della presentazione**:
  - Sintesi del percorso: micro-grid energetica distribuita, cluster Apache Pekko con sharding e singleton, data plane ad alta frequenza su Kafka, event sourcing su Redis e failover verificato con chaos testing.
  - Ringraziamento della commissione e apertura immediata a **Domande (Q&A)** e **Live Demo**.
- **Cluster già pronto in background**: Docker Compose attivo con 3 nodi backend, Caddy, Kafka e Redis, pronto per il lancio della GUI ScalaFX.

### Traccia discorsiva
> "In conclusione, GridSim-Distributed mostra come sia possibile conciliare un modello fisico deterministico con le sfide tipiche dei sistemi distribuiti: abbiamo separato nettamente il controllo dalla telemetria ad alta frequenza, esternalizzato lo stato con Event Sourcing su Redis senza lock distribuiti, e garantito consistenza e resilienza grazie allo sharding e allo Split Brain Resolver di Pekko.
> Vi ringraziamo per l'attenzione: siamo a vostra completa disposizione per le domande e per mostrarvi il cluster in azione nella Live Demo."

---

### Appendice: Checklist Rapida per la Live Demo (se richiesta dai docenti)

---

## Possibili Domande d'Esame (Q&A Cheat Sheet)

- **D: "Cosa succede se cade Redis mentre la simulazione sta girando?"**
  - **R:** Gli attori continuano a ricevere il comando `Evolve`, ma quando provano a eseguire `Effect.persist(...)` la scrittura su Redis fallisce. Scatta quindi la strategia di supervisione `.onPersistFailure(SupervisorStrategy.restartWithBackoff(200ms, 5s))`: l'attore viene riavviato con backoff esponenziale e, nel frattempo, il `SimulationActor` va in timeout sul tick, entra in retry esponenziale e dopo 3 tentativi segnala lo stato `DEGRADED` alla GUI senza corrompere lo stato. Appena Redis torna raggiungibile, gli attori completano il recovery (`RecoveryCompleted`), ri-armano il timer e riprendono dall'ultimo tick confermato.
- **D: "Perché avete usato sia HTTP che Kafka? Non potevate fare tutto con Kafka o tutto con WebSocket?"**
  - **R:** I due canali hanno semantiche opposte: i comandi di controllo (`create`, `start`, `pause`) sono interazioni punto-punto di tipo *request/reply* (dove il client vuole sapere subito l'ID della simulazione creata o se il preset è invalido), per cui HTTP REST è perfetto. La telemetria invece è un flusso *one-to-many* ad alta frequenza: usando Kafka disaccoppiamo completamente il backend dai consumatori, permettendo a $N$ GUI di collegarsi e fare anche *replay* dei tick passati grazie alla retention del log di Kafka, cosa che con un semplice WebSocket richiederebbe di mantenere buffer e stato delle sessioni dentro il backend.
- **D: "Come garantite che due nodi del cluster non modifichino contemporaneamente la stessa entità in Redis?"**
  - **R:** Tramite la combinazione di **Pekko Cluster Sharding** (che garantisce il *Single-Writer Invariant*, allocando ogni `entityId` su un solo shard/nodo alla volta) e dello **Split Brain Resolver (`keep-majority`)**, che in caso di partizione di rete abbatte il lato minoritario prima che il lato maggioritario possa ri-allocare gli shard degli attori isolati.
