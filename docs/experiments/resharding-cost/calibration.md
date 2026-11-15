# Kalibracja sprzętu (2026-11-14 23:25 UTC)

Wyniki `hack/calibrate.py`; opis metody w nagłówku skryptu. Liczby z jednego przebiegu.

## Sieć (Pod → Pod, flannel VXLAN, bez TLS)

Karty sieciowe (node-exporter, łącze w górze):

| Węzeł (instance) | Karta | Mbit/s |
|---|---|---|
| 10.0.0.51:9100 | eno1 | 1000 |
| 10.0.0.50:9100 | eno1 | 100 |
| 10.0.0.49:9100 | eno1np0 | 10000 |
| 10.0.0.49:9100 | eno3 | 100 |
| 10.0.0.53:9100 | eno1 | 1000 |

| Z | Do | iperf3 Gbit/s | MB/s | RTT śr. ms | RTT maks. ms |
|---|---|---|---|---|---|
| sad-s24 | u0c-uk3 | 0.69 | 87 | 0.231 | 0.337 |
| sad-s24 | wdp-fwu | 0.70 | 87 | 0.217 | 0.344 |
| u0c-uk3 | sad-s24 | 0.90 | 112 | 0.268 | 1.054 |
| u0c-uk3 | wdp-fwu | 0.90 | 112 | 0.414 | 3.236 |
| wdp-fwu | sad-s24 | 0.90 | 112 | 0.214 | 0.314 |
| wdp-fwu | u0c-uk3 | 0.86 | 108 | 0.333 | 1.641 |

## Jeden rdzeń Redisa (redis-benchmark w tym samym Podzie, bez TLS, SET 1000 B, 50 klientów)

| Węzeł | SET/s pipeline 16 | SET/s pipeline 1 | intrinsic latency |
|---|---|---|---|
| sad-s24 | 481812 | 42998 | Max latency so far: 406 microseconds |
| u0c-uk3 | 548546 | 67644 | "SET","67643.74","0.383","0.176","0.367","0.455","0.695","7.519" |
| wdp-fwu | 1040042 | 85886 | Max latency so far: 158 microseconds |

## Dysk (`longhorn-nvme-local`, replika na węźle Poda)

| Węzeł | Replika | zapis 4 KiB + fsync śr. ms | p99 ms | zapis sekwencyjny 1 MiB MB/s |
|---|---|---|---|---|
| sad-s24 | talos-sad-s24:running | 1.15 | 2.31 | 145 |
| u0c-uk3 | talos-u0c-uk3:running | 1.78 | 2.70 | 137 |
| wdp-fwu | talos-wdp-fwu:running | 0.52 | 1.43 | 289 |

## Wywołania Instance Managera (pilot, 2026-11-14 21:33:30–21:38:10 UTC)

Z pierwszego przebiegu kalibracji (sieć wtedy: `u0c-uk3` na 100 Mbit/s; wywołania są małe, RTT bez zmian).

| Ścieżka | śr. ms | wywołań |
|---|---|---|
| /cluster/importing | 1.63 | 8078 |
| /cluster/migrate | 1.59 | 8078 |
| /cluster/setnode | 2.30 | 16156 |
| /config | 1.33 | 80 |
| /status | 1.33 | 65500 |

## Wnioski

- **Sieć po naprawie:** `u0c-uk3` na `eno1np0` 10 Gbit/s; każda para węzłów 0,7–0,9 Gbit/s
  (sufit to 1 GbE pozostałych węzłów), RTT 0,21–0,41 ms. Przed naprawą `u0c-uk3` szedł przez
  `eno3` na 100 Mbit/s (~10 MB/s). `eno3` dalej jest w górze; ruch idzie już przez 10G.
- **Rdzeń:** `wdp-fwu` najszybszy (SET z pipeline 16: ~1,0 mln/s), `u0c-uk3` ~0,55 mln/s,
  `sad-s24` ~0,48 mln/s. Węzeł z 56 CPU ma wolniejsze pojedyncze rdzenie.
- **Dysk:** fsync 0,5–1,8 ms, zapis sekwencyjny 137–289 MB/s; `wdp-fwu` najszybszy.
  Replika woluminu zawsze na węźle Poda.
- **Wywołania IM potwierdzają model `c_slot`:** na slot 1× importing (1,63 ms) + 1× migrate
  (1,59 ms) + 2× setnode (2,30 ms) + 8,1× status (1,33 ms) = 18,6 ms; zmierzone 19,5 ms
  (pilot, 8 węzłów). Pozostałe ~1 ms to planowanie i odczyt Podów z cache.
- Intrinsic latency `u0c-uk3`: skrypt nie złapał linii wyniku (do poprawki, nieistotne dla
  reshardingu).
