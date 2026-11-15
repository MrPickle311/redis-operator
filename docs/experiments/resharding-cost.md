# Eksperyment: ile kosztuje resharding Redis Cluster (posty 1 i 8)

Pytanie czytelnika: „ile potrwa resharding mojego klastra i czy klienci coś
poczują?”. Wynik ma dać wzór, który czytelnik policzy dla siebie, i sposób,
jak w 10 minut zmierzyć jego stałe na własnym klastrze.

## Co wiemy przed eksperymentem

Protokół jednego slotu jest Redisa i ten sam w każdym narzędziu
(`redis-cli --cluster reshard`, operatory): `SETSLOT IMPORTING` na celu,
`SETSLOT MIGRATING` na źródle, `GETKEYSINSLOT` + `MIGRATE` w partiach,
`SETSLOT NODE` na obu. Nasze wybory, które zmieniają koszt:

| Wybór | Wartość | Gdzie |
|---|---|---|
| sloty naraz | 1 w całym klastrze | `moveSlots`, `internal/controller/slotmigration.go` |
| klucze w jednym `MIGRATE` | 100 (`migrateBatch`) | `internal/instancemanager/cluster.go` |
| timeout `MIGRATE` | 5000 ms | `clusterMigrate` |
| praca w jednej rekoncyliacji | 5 s (`reshardBudget`), potem zapis statusu i 1 s przerwy | `slotmigration.go` |
| po każdym slocie | odczyt stanu **wszystkich** węzłów (`clusterNodes`) | `moveSlots` |
| stan migracji | tylko flagi w Redisie (`nodes.conf`), operator nic nie pamięta | `slot-migration-walkthrough.md` |

Dane z krzywej skalowania (100 tys. kluczy po 256 B, ~6 kluczy na slot):
51 / 86 / 119 ms na slot przy 12 / 18 / 24 węzłach, czyli około 5 ms na
węzeł na slot.

## Hipotezy

- **H1. Przy małych danych czas zależy od liczby slotów i liczby węzłów, a nie
  od danych.** Koszt slotu ≈ 5 ms × liczba węzłów, bo operator po każdym
  slocie odpytuje wszystkie węzły. Obalona, jeśli przy stałych danych koszt
  slotu nie rośnie z liczbą węzłów.
- **H2. Czas reshardingu to suma dwóch części:**
  `T ≈ sloty × c_slot + bajty / B` (koszt koordynacji plus transfer). Z tego
  wynika punkt przegięcia, od którego liczą się dane. Obalona, jeśli czas nie
  układa się w taką prostą (np. rośnie z liczbą kluczy, a nie bajtów).
- **H3. Klient obsługujący cluster nie widzi błędów, tylko wyższe opóźnienie.**
  MOVED i ASK to przekierowania. `MIGRATE` blokuje główny wątek źródła na czas
  jednej partii, więc p99 w trakcie ≈ p99 przed + czas partii. Obalona przy
  pierwszym nieudanym żądaniu albo timeoucie.

## Warunki ważności (do sprawdzenia przed każdym punktem)

- Obciążenie stałe i umiarkowane: główny wątek masterów ≤ 50 % przed
  reshardingiem (`hack/prom-window.py`). Porównujemy p99 w trakcie z p99 przed,
  przy tym samym obciążeniu.
- Zmienia się jedna rzecz naraz: rozmiar danych albo rozmiar wartości.
- Klaster zbalansowany i Ready przed startem, dane wczytane w całości (liczba
  kluczy sprawdzona `DBSIZE` na masterach).
- Każdy punkt co najmniej 2 razy; w tabeli mediana i rozrzut.
- Mierzymy w obie strony (3→4 i 4→3), więc powrót też jest punktem pomiarowym.

## Profil klastra

30 GB = łączny rozmiar klastra z replikami: 15 GB danych na masterach,
15 GB kopii na replikach.

- Nowy `RedisCluster` `bulk` w `redis-play` (osobno od `store`, żeby nie
  mieszać wyników): 3 shardy × 2 instancje, `limits.memory: 7Gi`
  (`maxmemoryRatio` 75 → ~5,25 GB na mastera, przy 3 shardach 15 GB danych to
  5 GB), PVC 20Gi (AOF + przepisywanie), `monitoring.enabled: true`.
- W szczycie 3→4: 8 Podów × 7Gi = 56 GiB limitów. Przed startem sprawdzić
  wolną pamięć węzłów (`kubectl describe nodes`); jeśli brakuje, tymczasowo
  wyłączyć `applications` (ArgoCD: najpierw auto-sync).

## Punkty pomiarowe

| Punkt | Dane | Wartość | Kluczy | Kluczy na slot | Po co |
|---|---|---|---|---|---|
| P0 | ~0 | – | 0 | 0 | sam koszt koordynacji |
| P1 | 1 GB | 1 KB | 1 mln | ~61 | |
| P2 | 5 GB | 1 KB | 5 mln | ~305 | |
| P3 | 15 GB | 1 KB | 15 mln | ~915 | |
| P4 | 15 GB | 10 KB | 1,5 mln | ~92 | te same bajty co P3, 10× mniej kluczy: bajty czy klucze? |
| P5 | 0 | – | 0 | 0 | 3→6 i 3→9 na pustym klastrze: c_slot względem liczby węzłów (H1) |

## Przebieg jednego punktu

1. Wczytać dane (memtier `--ratio=1:0 -n allkeys --key-pattern=P:P`), poczekać,
   aż repliki się zsynchronizują.
2. Puścić stałe obciążenie (memtier, 2 Pody, odczyty i zapisy na kluczach
   danych, żeby część trafiała w sloty w drodze, czyli w ASK). 2 min bazy.
3. `spec.shards = 4`, czekać na `Balanced` i Ready. Zapisać czas.
4. 1 min po, potem `spec.shards = 3`, znowu czekać i zapisać czas.
5. Zebrać:
   - czas całości i sloty/s;
   - koszt każdego slotu: znaczniki czasu linii `Moved slot` z logów
     operatora, rozkład ms na slot względem liczby kluczy w slocie;
   - MB/s transferu (ruch wyjściowy źródeł, `prom-window.py`);
   - p50/p99 i MOVED/ASK z memtiera, błędy i timeouty;
   - główny wątek źródła i celu, sieć, dysk: werdykt `prom-window.py`.

Opcjonalnie dwa porównania:
- **Narzędzie Redisa:** ten sam punkt P3 przez `redis-cli --cluster reshard`
  przy zatrzymanym operatorze (inaczej wyrówna sloty po swojemu).
- **Poprawka operatora:** po każdym slocie `moveSlots`
  (`slotmigration.go:104`) odpytuje wszystkie węzły (~5 ms każdy), choć
  zmieniły się tylko źródło i cel. Odświeżać tylko te dwa (albo poprawić
  widok w pamięci). Przewidywanie: slot z ~45 ms do ~10–15 ms, P0 z ~3 min
  do <1 min. Powtórzyć P0 i P5: przed/po.

Czas: wczytanie ~2–5 min na punkt, resharding 3–10 min w każdą stronę;
razem 2–3 godziny klastra, w większości bez nadzoru.

## Spodziewany wynik

Liczby to przewidywania z kodu i z krzywej skalowania; po pomiarze wpisać
obok prawdziwe.

| Punkt | Przewidywany czas 3→4 | Dlaczego |
|---|---|---|
| P0 | ~3 min (4096 slotów × ~40–48 ms) | 8 węzłów × ~5 ms; tyle samo trwało 3→4 wcześniej (3 min 15 s) |
| P1–P3 | +2–3 min przy 15 GB | 3,75 GB do przeniesienia; przy ~50 MB/s to ~75 s transferu plus ~37 tys. partii po 1–2 ms |
| P4 | krócej niż P3 | 10× mniej partii przy tych samych bajtach |
| P5 | c_slot rośnie liniowo z węzłami | H1 |

- **Punkt przegięcia:** przy c_slot ~45 ms i B ~50 MB/s dane zaczynają
  dominować dopiero od ~8 GB przenoszonych, czyli ~32 GB danych przy 3→4.
  Przy 15 GB (3,75 GB w ruchu) koordynacja nadal przeważa, ale transfer to
  już ~1/3 czasu. Jeśli B wyjdzie dużo niższe
  (np. 10 MB/s przez TLS i synchroniczny `MIGRATE`), punkt przegięcia spadnie
  do kilku GB i H2 pokaże to od razu.
- **Klienci:** 0 błędów, p99 wyższe o czas jednej partii (100 × 1 KB to
  ~1–2 ms; 100 × 10 KB w P4 wyraźnie więcej), dziesiątki MOVED/ASK na sekundę.
- Każda z tych liczb może wyjść inaczej; ciekawsze dla tekstu jest to, która
  hipoteza padnie.

## Spodziewane wnioski (do posta)

1. **Do kilkudziesięciu GB koszt reshardingu to koordynacja, nie dane.** Czas
   zależy od liczby slotów i od tego, jak narzędzie je przenosi, a nie od
   rozmiaru zbioru. Wzór `T ≈ sloty × c_slot + bajty / B` i sposób zmierzenia
   obu stałych u siebie.
2. **Szczegóły narzędzia zmieniają koszt kilkukrotnie.** U nas odczyt stanu
   wszystkich węzłów po każdym slocie: klaster 2× większy ma 2× droższy slot.
   Uczciwy wątek o własnym kodzie, a z poprawką: wynik przed/po.
3. **Duże wartości przesuwają koszt na transfer i blokowanie źródła.** Partia
   po 100 kluczy to 100 KB przy 1 KB, ale 1 MB przy 10 KB, czyli dłuższe
   zatrzymanie głównego wątku i wyższe p99. To prowadzi do posta o dużych
   kluczach (#2).
4. **Klient z obsługą cluster przeżywa resharding bez błędów**, płaci
   opóźnieniem. Konkretnie: ile.

## Opisywać nasz algorytm?

Tak, ale jako studium przypadku, nie dokumentację:
- protokół czterech poleceń jest Redisa i dotyczy każdego narzędzia;
- nasze wybory (slot naraz, 100 kluczy w partii, budżet 5 s, odczyt stanu
  po slocie, brak własnego stanu) to pokrętła, a pomiar pokazuje, które z nich
  kosztują;
- szczegóły kodu: link do `slot-migration-walkthrough.md`.
