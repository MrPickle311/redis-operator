# Wnioski: koszt reshardingu

Wnioski po każdym etapie. Surowe wiersze: `results.tsv`, z linkami do Grafany: `results.md`,
logi przebiegów: `logs/`. Sprzęt: `calibration.md`. Plan: artefakt „Eksperyment: ile kosztuje
resharding Redis Cluster”.

## Etap 0: pilot (2026-11-14)

- Cały łańcuch działa: skrypt, Loki (czasy linii `Moved slot`), panele, podział `wait`/`move`.
- `c_slot` 19,5 ms przy 8 węzłach, a nie ~45 ms z wcześniejszej krzywej skalowania. Tamte ~5 ms
  na węzeł pochodziły z klastra `store` z limitem CPU 250m, który dławił też Instance Managera.
- Przerw po 1 s (`RequeueAfter`) prawie nie ma: zapis statusu po 5 s budżetu od razu wywołuje
  kolejną rekoncyliację.
- Pojemność: `sie-ons` to control-plane z taintem `NoSchedule`. Przy 6Gi mieści się 10 Podów
  (sad 1, u0c 7, wdp 2): P6 (12 Podów) wymaga limitu 5Gi i ~8 GB danych.

## Etap 0b: kalibracja (2026-11-15)

Szczegóły w `calibration.md`.

- `u0c-uk3` szedł przez kartę 100 Mbit/s (~10 MB/s); po naprawie przez 10 Gbit/s. Każda para
  węzłów 0,7–0,9 Gbit/s (sufit: 1 GbE pozostałych węzłów), RTT 0,21–0,41 ms.
- Węzeł z 56 CPU (`u0c-uk3`) ma wolniejsze pojedyncze rdzenie niż `wdp-fwu` (~0,55 vs ~1,0 mln
  SET/s z pipeline 16).
- Model `c_slot` z wywołań IM: 1× importing (1,63 ms) + 1× migrate (1,59) + 2× setnode (2,30)
  + 8× status (1,33) = 18,6 ms; zmierzone 19,5 ms.

## Etap 1: bez danych, V0 (2026-11-15 10:30–12:31 UTC)

20 przebiegów: P0 (3↔4) 4×, P5 (3↔6, 3↔9) po 3×, w obie strony, przeplatane.

| Kierunek | Węzły | Sloty | n | `c_slot` mediana [min–max] | `move` mediana | `wait` mediana |
|---|---|---|---|---|---|---|
| 3→4 | 8 | 4096 | 4 | 19,4 ms [19,2–19,6] | 89 s | 31 s |
| 4→3 | 8 | 4096 | 4 | 20,1 ms [19,1–20,2] | 91 s | 0,3 s |
| 3→6 | 12 | 8192 | 3 | 25,3 ms [24,9–25,4] | 231 s | 43 s |
| 6→3 | 12 | 8192 | 3 | 25,1 ms [24,7–25,5] | 229 s | 0,4 s |
| 3→9 | 18 | 10923 | 3 | 35,6 ms [35,6–35,7] | 427 s | 82 s |
| 9→3 | 18 | 10923 | 3 | 36,2 ms [36,2–36,3] | 430 s | 0,4 s |

- **Powtarzalność:** rozrzut między przebiegami ≤ ~5%; oba kierunki symetryczne. Od etapu 2
  wystarczą 2 powtórzenia (trzecie przy rozrzucie > 10%).
- **H1 potwierdzona:** `c_slot` rośnie z liczbą węzłów: ~1,6 ms na węzeł + ~7 ms stałe
  (4 wywołania IM ≈ 7,8 ms z kalibracji).
- **Lekko szybciej niż liniowo:** 1,4 ms/węzeł między 8 a 12 węzłami, 1,8 ms/węzeł między 12 a
  18. Hipoteza: każdy `/status` parsuje `CLUSTER NODES` z N liniami, więc koszt odczytu też rośnie
  z N i całość dąży do N². Do sprawdzenia w V1 (po slocie tylko 2 odczyty).
- **Umiejscowienie bez wpływu bez danych:** P0 z wszystkimi slotami w obrębie jednej maszyny
  (r1) i przez sieć (r2–r4) daje ten sam `c_slot`.
- **`wait`** (start nowych Podów, woluminy, MEET): 31 s dla 1 sharda, 43 s dla 3, 82 s dla 6;
  przy usuwaniu shardów ~0. Nie należy do kosztu przenoszenia slotów.
- **Brak przerw** między rekoncyliacjami we wszystkich 20 przebiegach.
- **Po restarcie klastra** `bulk-0-0` miał stare własne IP (ten sam błąd co po awarii
  2026-11-14, zadanie w `redis-operator-master-plan.md`, sekcja 20); naprawione ręcznie
  `CLUSTER MEET` przed etapem. `check` wykrywa to teraz i zatrzymuje etap.

## Etap 2: bez danych, V1 i V2 (2026-11-15 12:56–14:59 UTC)

28 przebiegów: w każdym z 2 powtórzeń P0 na V0 (kontrola), potem P0, P5 3↔6 i P5 3↔9 na V1 i V2
na przemian, w obie strony. Dane kompletne: znaczniki START/END i linie `Moved slot` dla każdego
przebiegu, metryki operatora i exportera co 1 s (exporter 86–96% przy zwiększaniu klastra, bo
nowe Pody startują w trakcie okna).

| Węzły | Wariant | n | `c_slot` mediana [min–max] | p90 | `move` mediana |
|---|---|---|---|---|---|
| 8 | V0 (etap 1) | 8 | 19,6 ms [19,1–20,2] | 25,2 ms | 90 s |
| 8 | V0 (kontrola etapu 2) | 4 | 18,9 ms [17,8–19,9] | 24,5 ms | 87 s |
| 8 | V1 | 4 | 11,1 ms [10,0–12,5] | 14,6 ms | 51 s |
| 8 | V2 | 4 | 12,1 ms [11,7–12,3] | 22,0 ms | 62 s |
| 12 | V0 (etap 1) | 6 | 25,2 ms [24,7–25,5] | 34,7 ms | 231 s |
| 12 | V1 | 4 | 11,7 ms [10,7–12,2] | 14,9 ms | 106 s |
| 12 | V2 | 4 | 12,9 ms [12,7–13,1] | 33,4 ms | 149 s |
| 18 | V0 (etap 1) | 6 | 36,0 ms [35,6–36,3] | 48,5 ms | 430 s |
| 18 | V1 | 4 | 11,6 ms [11,0–12,1] | 14,9 ms | 136 s |
| 18 | V2 | 4 | 14,3 ms [13,9–14,8] | 51,0 ms | 242 s |

- **H3 potwierdzona:** V1 (po slocie `/status` tylko źródła i celu) ma `c_slot` ~11–12 ms
  niezależnie od liczby węzłów. Model: 4 wywołania IM (~7,8 ms) + 2 odczyty `/status` (~2,7 ms)
  = 10,5 ms. Przy 18 węzłach V1 przenosi sloty 3,2× szybciej niż V0 (136 s zamiast 430 s).
- **V2 (wszystkie węzły równolegle) jest gorszy od V1:** mediana zbliżona (12–14 ms), ale ogon
  rośnie z liczbą węzłów (p90 22 → 33 → 51 ms), bo każdy slot czeka na najwolniejszy z N
  odczytów. Średnio V2 przy 18 węzłach jest 1,8× wolniejszy od V1. Wniosek: nie czytać stanu,
  którego nie potrzeba, zamiast czytać wszystko szybciej.
- **Kontrola V0 bez dryfu:** P0 na V0 w etapie 2 (17,8–19,9 ms) w rozrzucie etapu 1.
- **Po V1 dominuje `wait`:** przy 3→9 start 6 nowych shardów (~80 s) to już ~40% całego
  reshardingu bez danych.
- Operator po etapie wrócił na V0 (`kubectl set env` bez zmiennych wariantów).

## Etap 3: dane, układ L, V0 (2026-11-15 15:27–17:06 UTC)

Przed etapem: pamięć `bulk` 1Gi → 5Gi (rolling update). Po rolling update 5 z 6 węzłów miało
stare własne IP (ten sam błąd operatora; tym razem po zwykłym rolling update, nie po awarii),
naprawione `CLUSTER MEET` przed startem.

Rozmiar klucza: 1147 B na klucz przy wartości 1000 B (100 tys. kluczy); przy milionach kluczy
1110–1142 B. 16 przebiegów, po 2× w obie strony; układ L za każdym razem (wszystkie sloty w
obrębie `u0c-uk3`, `local_slots` = wszystkie). Rozrzut ≤ 3%.

| Punkt | Dane | Klucze/slot | Kierunek | `c_slot` mediana | dodatek do bez danych | µs/klucz | `move` | w ruchu | `B` |
|---|---|---|---|---|---|---|---|---|---|
| P1 | 0,91 GiB | 53 | 3↔4 | 23,2 ms | +3,5 ms | ~67 | 100 s | 0,24 GB | ~12 MB/s |
| P2 | 4,53 GiB | 266 | 3↔4 | 33,4 ms | +13,8 ms | ~52 | 140 s | 1,22 GB | ~20 MB/s |
| P3 | 7,27 GiB | 426 | 3↔4 | 41,4 ms | +21,8 ms | ~51 | 172 s | 1,95 GB | ~21 MB/s |
| P6 | 7,27 GiB | 426 | 3↔6 (12 węzłów) | 47,4 ms | +22,2 ms | ~52 | 392 s | 3,9 GB | ~21 MB/s |

- **Koszt slotu = koordynacja + dane, składniki się sumują:**
  `c_slot ≈ c_koord(N węzłów) + ~50 µs × klucze w slocie`. P3 (8 węzłów) i P6 (12 węzłów) mają
  ten sam dodatek za dane (+22 ms) przy innej koordynacji (19,6 vs 25,2 ms).
- **`B` ≈ 21 MB/s** przy 1 KB wartościach, nawet bez sieci (układ L). To ~19% możliwości sieci
  z kalibracji i ~15% zapisu sekwencyjnego dysku.
- **Wąskie gardło: synchroniczny `MIGRATE`, jedna partia naraz** (okno P3, 16:09–16:12):
  - główny wątek źródła i celu zajęty tylko 0,14–0,21 rdzenia, cały kontener ≤ 0,34: Redis nie
    jest nasycony;
  - jedno `MIGRATE` (100 kluczy, ~110 KB) trwa średnio 3,6 ms, `RESTORE-ASKING` na celu ~8 µs
    na klucz: większość czasu to czekanie na odpowiedź celu (TLS, round trip, zapis), a nie praca;
  - `/cluster/migrate` (cały slot, ~5 partii) 21,7 ms na wywołanie.
  Operator przenosi jeden slot naraz z jedną partią w locie, więc przepustowość ≈ 100 kluczy /
  3,6 ms ≈ 28 tys. kluczy/s ≈ 30 MB/s, minus narzut `GETKEYSINSLOT` i HTTP do IM ≈ 21 MB/s.
- **Wniosek do posta:** przy 8 GB transfer to ~55% czasu 3→4 (94 z 172 s), koordynacja ~45%.
  Obie części skraca się innymi pokrętłami: koordynację odczytem stanu tylko dwóch węzłów (V1),
  dane większą partią albo kilkoma slotami naraz (więcej w locie), nie szybszą siecią.
- **`MIGRATE` blokuje główny wątek źródła na 3,6 ms co partię**, choć ten nie liczy: klienci
  źródła czekają tyle samo (pomost do posta o dużych kluczach i opóźnieniach).
- **Nowa hipoteza H4 (zapisana w planie, lista hipotez):** limitem jest synchroniczny `MIGRATE`
  z jedną partią w locie, więc V4 (`MIGRATE_BATCH=1000`) podnosi `B` co najmniej ~2×, a jedno
  `MIGRATE` (blokada źródła) rośnie z ~3,6 ms do kilkudziesięciu ms. Obalona, jeśli `B` prawie się
  nie zmieni. Sprawdzana w etapie 5 obok V3 (`MIGRATE_BATCH=10`): krzywa partia 10 / 100 / 1000.

## Etap 4a: dane, układ N (2026-11-15 17:33–18:12 UTC)

- **Błąd w prowadzeniu:** etap 4a wystartował dwa razy (pierwsze uruchomienie, uznane za nieudane,
  jednak ruszyło). Od 17:33 do 17:45 UTC dwa skrypty zmieniały `spec.shards` i mierzyły ten sam
  resharding. Sam resharding był jeden (robi go operator), ale powstały dwa błędne wiersze:
  `S4-P3-N-V0-r1-4to4` (17:35:08, drugi skrypt zastał już 4 shardy) i drugi
  `S4-P3-N-V0-r1-4to3` (17:40:26, start 3 s po właściwym, 4020 slotów). Usunięte z
  `results.tsv`/`results.md`; **ich znaczniki zostały w Loki** (tabela „Every run” i adnotacje w
  Grafanie pokazują je dodatkowo: pomijać). Ważne przebiegi r1: `3to4` 17:35:04, `4to3` 17:40:23.
  Drugi skrypt zatrzymany o 17:46 UTC. Po etapie dodana blokada (`flock`) w
  `hack/reshard-stages.sh`: drugi etap nie ruszy, póki działa pierwszy.
- Shard 2 ma oba Pody na `u0c-uk3` (odtworzona replika wróciła na ten sam węzeł), więc ~1/3
  slotów idzie lokalnie (`local_slots` ≈ 1366 z 4096). Po odtworzeniu repliki znowu stare własne IP.

6 ważnych przebiegów: P3-N 2× (3↔4), P6-N 1× (3↔6). Układ N: mastery shardów 0 i 1 na
`wdp-fwu` i `sad-s24`, shard 2 i nowe shardy na `u0c-uk3`, więc 2/3 slotów idzie przez sieć
(`local_slots` 1366 z 4096 i 2731 z 8192).

| Punkt | Kierunek | L (etap 3) `move` / `c_slot` | N `move` / `c_slot` | N vs L |
|---|---|---|---|---|
| P3 (8 GB) | 3→4 | 172 s / 41,4 ms | 157 s / 37,1 ms (r1, r2: 157,3 i 157,7) | −9% |
| P3 (8 GB) | 4→3 | 172 s / 41,5 ms | 165 s / 39,5 ms (r1, r2: 164,8 i 164,7) | −4% |
| P6 (8 GB) | 3→6 | 394 s / 47,6 ms | 366 s / 43,1 ms (r1) | −7% |
| P6 (8 GB) | 6→3 | 391 s / 47,2 ms | 387 s / 46,2 ms (r1) | −1% |

- **Przez sieć nie wolniej, raczej szybciej.** Sieć (0,7–0,9 Gbit/s) nie ogranicza transferu przy
  `B` ≈ 21 MB/s; to zgodne z etapem 3 (limitem jest synchroniczny `MIGRATE`, nie przepustowość).
- **Skąd zysk w N (hipoteza, nie sprawdzona):** w L źródło i cel (oba z TLS) i ich praca dzielą
  jeden węzeł `u0c-uk3`; w N część źródeł stoi na `wdp-fwu`, który ma najszybszy rdzeń z
  kalibracji. Zysk największy przy 3→4 (źródła: `wdp-fwu`, `sad-s24`), najmniejszy przy 6→3
  (źródła na `u0c-uk3`). Do potwierdzenia panelem *Main thread busy* w oknach przebiegów.
- **Wniosek do posta:** przy 1 KB wartościach i jednej partii w locie położenie masterów i sieć
  1 GbE zmieniają czas reshardingu o kilka procent; liczy się liczba kluczy i to, ile `MIGRATE`
  jest w locie.
- Do zrobienia w 4b: P6-N r2, potem P4 (10 KB wartości) w N i L.

## Etap 4b: P6-N r2 i P4 (10 KB wartości), układy N i L (2026-11-15 18:39–19:32 UTC)

10 przebiegów, bez problemów. Dane etapu 4 (4a + 4b) kompletne: 16 ważnych przebiegów, linie
`Moved slot` i metryki operatora pełne w każdym oknie.

P6-N r2: 3→6 368 s / 43,5 ms, 6→3 385 s / 46,0 ms (jak r1: 366 i 387 s).

P4: rozmiar klucza 10 325 B przy wartości 10 000 B (narzut ~325 B). 774 818 kluczy, 7,46 GiB:
te same bajty co P3 (7,27 GiB), 9× mniej kluczy (47 zamiast 426 na slot).

| Punkt | Układ | Klucze/slot | `move` 3→4 / 4→3 | `c_slot` | dodatek za dane | `B` |
|---|---|---|---|---|---|---|
| P3 (1 KB) | L | 426 | 172 / 172 s | 41,4 ms | +21,8 ms | ~21 MB/s |
| P3 (1 KB) | N | 426 | 157 / 165 s | 37,1–39,5 ms | +17,5–19,9 ms | ~25 MB/s |
| P4 (10 KB) | N | 47 | 108 / 110 s (r1, r2: 108,6 i 107,4; 110,0 i 109,2) | 24,4–25,2 ms | +5,1 ms | ~70 MB/s |
| P4 (10 KB) | L | 47 | 113 / 112 s (r1, r2: 111,9 i 113,1; 111,7 i 112,5) | 26,1–26,5 ms | +6,6 ms | ~60 MB/s |

- **H2 obalona:** przy tych samych bajtach czas rośnie z liczbą kluczy. P4 (10× mniej kluczy)
  przenosi dane ~3× szybciej niż P3; resharding 3→4 przy 8 GB trwa 108–113 s zamiast 157–172 s.
- **Model kosztu danych na slot** (z P3-N i P4-N, dwa równania): ~33 µs na klucz (stały koszt
  klucza w `MIGRATE`: serializacja, RESTORE, nagłówki) + ~7 µs na KB wartości (~140 MB/s czystego
  transferu). Przy 1 KB wartościach koszt klucza to ~80% kosztu danych, przy 10 KB ~25%.
- **Pełny wzór na czas przenoszenia:**
  `move ≈ sloty × (c_koord(N) + 33 µs × klucze/slot + 7 µs × KB/slot)`, gdzie
  `c_koord ≈ 7 ms + 1,6 ms × liczba węzłów` (V0) albo ~11 ms (V1).
- **Układ L vs N przy 10 KB:** różnica ~3% (N nieco szybszy), mniejsza niż przy 1 KB. Sieć 1 GbE
  nadal nie ogranicza nawet przy ~70 MB/s.
- **Do posta:** bardziej boli wiele małych kluczy niż te same bajty w dużych kluczach. Rozmiar
  zbioru w GB to zła miara czasu reshardingu; dobra to liczba kluczy (i liczba węzłów przez
  koordynację). Duże klucze mają za to inną cenę: dłuższą blokadę źródła w jednym `MIGRATE`
  (pomost do posta o dużych kluczach).

## Etap 5: warianty partii i redis-cli (2026-11-15 19:45–21:28 UTC)

- **Przerwa 20:25 UTC:** jedno `kubectl get` do API zwróciło chwilowy błąd w trakcie
  `S5-P3-L-V3-r2-3to4`; skrypt się przerwał, blokada przełączyła operatora z V3 na V0 w trakcie
  reshardingu. Przebieg odrzucony (ma tylko znacznik START w Loki, bez wiersza w wynikach).
  Klaster wrócił do 3 shardów bez pomiaru (~20:35 UTC). Poprawka: odczyty z API ponawiane do 5×
  (`hack/reshard-cost.py`, `sh`), wznowienie etapu `SKIP_PREP=1 REP_FROM=2`.

16 ważnych przebiegów na P3 (8 GB, 1 KB wartości, 426 kluczy na slot), układ L. Kontrola V0 na
początku i na końcu: 167,8–173,0 s (etap 3: 172 s), bez dryfu. Przebiegi `redis-cli` tylko po
jednym powtórzeniu (r2), bo wznowienie od `REP_FROM=2` objęło też je; oba kierunki zgodne
(różnica ≤ 3%). Po `redis-cli` operator nie musiał wyrównać ani jednego slotu.

| Narzędzie | Partia (klucze w `MIGRATE`) | `move` 3→4 / 4→3 | `c_slot` | `B` | `MIGRATE` śr. / p99 | wpisy slowlog (> 10 ms) |
|---|---|---|---|---|---|---|
| operator V3 | 10 | 264 / 262 s | 65,4 ms | ~11 MB/s | 0,67 / 1,1 ms | 6 |
| operator V0 | 100 | 170 / 170 s | 41,3 ms | ~21 MB/s | 3,5 / 8,4 ms | 87 |
| operator V4 | 1000 | 141 / 142 s | 34,2 ms | ~32 MB/s | 11,7 / 27,4 ms | 2907 |
| `redis-cli` R10 (domyślnie) | 10 | 221 / 226 s | 54,5 ms (średnio) | – | 0,68 / 1,1 ms | 21 |
| `redis-cli` R100 | 100 | 116 / 112 s | 27,8 ms (średnio) | – | 3,4 / 8,3 ms | 48 |

(MIGRATE i slowlog z Prometheusa w oknie jednego przebiegu 3→4 każdego wariantu.)

- **H4 potwierdzona co do kierunku, nie co do skali:** partia 10 → 100 → 1000 skraca slot
  65 → 41 → 34 ms, `B` rośnie ~11 → 21 → 32 MB/s. Przewidywane ≥ 2× między 100 i 1000 wyszło
  1,5×. Każde dodatkowe `MIGRATE` kosztuje ~0,6 ms (round trip), ale przy jednym wywołaniu na
  slot zostaje ~34 µs na klucz (serializacja, RESTORE, AOF), którego partia już nie zmniejsza.
- **Cena większej partii jest realna:** jedno `MIGRATE` z 1000 kluczy blokuje główny wątek
  źródła średnio na 11,7 ms (p99 27 ms) zamiast 3,5 ms (p99 8 ms); ~2900 wpisów w slowlogu
  (> 10 ms) w jednym przebiegu zamiast 87. Klienci źródła czekają tyle samo.
- **`redis-cli` vs operator przy tej samej partii:** `redis-cli` szybszy o ~35% (partia 100:
  114 vs 170 s) i ~15% (partia 10: 224 vs 263 s). `MIGRATE` trwa tyle samo, więc różnica to
  koordynacja: `redis-cli` nie czyta stanu wszystkich węzłów po każdym slocie i nie idzie przez
  HTTP do Instance Managera. Szacunek: koordynacja `redis-cli` ~6–8 ms na slot, operatora V0
  19,6 ms (V1 ~11 ms, czyli V1 + partia 100 powinien zbliżyć się do `redis-cli`; nie mierzone).
- **Domyślne `redis-cli --cluster rebalance` (partia 10) jest wolniejsze od operatora z partią
  100** (224 vs 170 s): przy małych kluczach warto podnieść `--cluster-pipeline`.
- **Do posta:** dwa pokrętła z kompromisami. Koordynacja (ile stanu czyta narzędzie po slocie)
  kosztuje tylko czas; partia `MIGRATE` kosztuje czas albo przerwy klientów źródła. Partia 100
  to rozsądny środek: ~1,2× wolniej niż 1000, ale 3× krótsza blokada źródła.

**Znaczniki INVALID (dopisane 2026-11-15 ~21:50 UTC).** W Loki `{job="reshard-cost"}` dodane linie
`event=INVALID` z powodem (`note`) i oknem dla przebiegów do pominięcia; tabela „Every run” pokazuje
je na czerwono, adnotacje „Experiment runs” jako `INVALID <run> <powód>`:
- `S4-P3-N-V0-r1-4to4` (17:35:08) i drugi `S4-P3-N-V0-r1-4to3` (start 17:40:26): podwójny skrypt.
  Ich znaczniki END zostały (wyglądają jak zwykłe wiersze); ważny 4→3 to ten ze startem 17:40:23.
- `S5-P3-L-V3-r2-3to4` (start 20:23:51): przerwany. Resharding trwał 20:24:30–20:29:40 (Prometheus:
  `redis_operator_slots_moved_total`), ale **linii `Moved slot` z tego okresu nie ma w Loki**
  (zginęły, prawdopodobnie przy wymianie Poda operatora w trakcie). Ważne powtórzenie: start 20:38:41.
- `manual-4to3` (20:33:40–20:36:45): ręczny powrót do 3 shardów po przerwaniu, bez pomiaru.

## Etap 6: V1 z danymi i redis-cli r1 (2026-11-15 21:40–22:24 UTC)

10 przebiegów na P3 (8 GB, 1 KB, układ L), bez problemów. Przebiegi `redis-cli` mają numerację
etapu 5 (`S5-P3-L-R100-r1-…`), żeby pasowały do r2.

Klaster tego wieczoru był ~7–9% szybszy niż w etapach 3 i 5 (kontrola V0: 155 / 159 s zamiast
~170 s; `redis-cli` r1 o 7–9% szybszy niż r2). Dlatego porównania w obrębie jednej sesji i jako
stosunek do kontroli V0:

| Wariant | `move` 3→4 / 4→3 | `c_slot` | względem V0 z tej samej sesji |
|---|---|---|---|
| V0 (kontrola, etap 6) | 155 / 159 s | 38,5 / 39,3 ms | 1,00 |
| **V1 (stan tylko źródła i celu)** | 121 / 121 s (r1: 122,3 i 121,3; r2: 118,8 i 120,8) | 29,3–30,6 ms | **0,77** |
| `redis-cli` R100 r1 | 108 / 104 s | 26,5 / 25,3 ms (średnio) | 0,68 |
| `redis-cli` R10 r1 | 202 / 206 s | 49,3 / 50,2 ms (średnio) | 1,30 |

`redis-cli` r2 (etap 5) względem swojej kontroli V0 (~170 s): R100 0,67, R10 1,32: ten sam
stosunek co r1, więc wynik `redis-cli` jest powtarzalny po uwzględnieniu dryfu klastra.

- **V1 z danymi skraca resharding o ~23%** (przy 8 GB, 8 węzłach). `c_slot` ≈ 30 ms = ~11 ms
  koordynacji V1 + ~20 ms danych: dokładnie suma składników z etapów 2 i 3.
- **V1 prawie dogania `redis-cli` z tą samą partią** (0,77 vs 0,68 kontroli): zostaje ~3–4 ms na
  slot różnicy, czyli 4 wywołania HTTP do Instance Managera (~1,3–2,3 ms każde z kalibracji) zamiast
  bezpośrednich poleceń Redisa.
- **Dryf między sesjami ~7–9%** (ten sam klaster, ten sam punkt, inny wieczór): dlatego każda
  sesja ma kontrolę V0, a porównania wariantów robić względem kontroli z tej samej sesji.
- Zaległe (nie robione, do decyzji): V3/V4 na P4, `redis-cli` na P0 (bez danych), V1 + V4 razem.
