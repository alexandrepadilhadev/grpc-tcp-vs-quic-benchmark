# Análise dos resultados — atualizada em 2026-10-07

Duas rodadas completas da matriz, no mesmo commit (`7f84f30`), na VM (Ubuntu, 6 vCPUs). Juntas, dão **50 repetições por transporte em cada um dos 9 cenários**. Carga: `--rps=2000 --concurrency=500 --duration=60s --warmup=10s`; nos cenários `-low`, `--rps=200`. Como ler cada métrica está em [`architecture.md` §6](architecture.md#6-métricas).

**Convenções:**
- Valores são **medianas entre repetições**. Latências em ms.
- **Δ (HL):** deslocamento de Hodges–Lehmann h3 − h2, com IC exato de ~95% entre colchetes. **Razão:** h3/h2 típica, com IC.
- **p:** Mann-Whitney U bilateral, **corrigido por Holm** dentro do cenário (`mwu_p_holm`).
- **δ:** delta de Cliff, de −1 (todo h3 < todo h2) a +1. |δ| ≥ 0,474 é efeito grande.

## Resumo

1. **Rede ideal (s1): equivalência na prática.** O h3 tem p99 16% menor (−0,35 ms); o h2 abre a conexão 0,5 ms mais rápido e gasta 11% menos CPU por RPC no servidor. Todas as diferenças ficam abaixo de 1 ms.
2. **Perda com carga baixa (s2-low, 200 RPC/s): o h3 vence em todos os níveis, e a vantagem cresce com a perda.** Os dois atendem 100% da demanda, então a latência mede o transporte:

   | Perda | p50 h2 / h3 | p99 h2 / h3 | Δ p99 (HL) [IC] |
   |---|---|---|---|
   | 1% | 11,9 / 11,4 ms | 32 / 27 ms | −5,5 [−5,6; −5,4] |
   | 3% | 17,1 / 11,4 ms | 98 / 27 ms | −71 [−106; −46] |
   | 5% | 26,8 / 11,4 ms | 302 / 33 ms | −266 [−308; −239] |

   O p50 do h3 fica em ~11,4 ms em qualquer nível de perda, perto do RTT de 10 ms. O do h2 dobra de 1% para 5%. É o head-of-line blocking do TCP: cada perda segura todos os streams da conexão.
3. **Perda com 2000 RPC/s (s2): o h3 atende de 1,29× a 3,61× mais RPCs.** O TCP satura antes: atende 78%, 33% e 17% da demanda, contra 100%, 84% e 60% do h3.
4. **Jitter (s3): mesma mediana, mas cauda 4× maior no h3.** O p50 do h3 é 7 ms menor, mas o p99 vai de 96 ms (h2) para 421 ms (h3), e o do h3 varia muito entre repetições (91 ms a 2,5 s). O h2 é estável. **Causa, confirmada com qlog (§5.1):** o quic-go declara perdas que não aconteceram, reduz a janela de congestionamento e não a restaura quando descobre o engano. O TCP do Linux tolera a mesma reordenação sem reduzir a janela.
5. **Apagão (s5): o h2 volta em 1 s em todas as 50 repetições; o h3 volta em 4–5 s em 45 de 50.** Fora do apagão, p50 e CPU empatam.
6. **Handshake: o h3 é ~1 RTT mais rápido sempre que há atraso de rede** (−9,6 a −10,8 ms com RTT de 10 ms; −47 ms no s3). Na rede ideal, onde o custo é só CPU, o h2 é 0,5 ms mais rápido.

---

## 1. Rodadas usadas

| Pasta em `results/` | Commit | Repetições por transporte | Observação |
|---|---|---|---|
| `2026-10-06-23-17-30` (rodada A) | `7f84f30` | 20, todos os cenários | ruído de CPU nas repetições 2–10 do s1 (§7) |
| `2026-10-07-06-59-47` (rodada B) | `7f84f30` | 30, todos os cenários | 1 nova tentativa automática de probe (`s2-loss-3-low/h2/probe-failed-rep-23`) |
| `pooled-2026-10-07` | — | 50 = A + B | análise conjunta (`analyze.py A B --out …`), base das §4–§6 |
| `2026-10-01-23-09-19` | `7f84f30` | 50, só s1 | junto com o s1 da rodada B na §3 |
| `2026-10-07-21-41-01` | `7f84f30` + qlog | 1, só s3, com `QLOG=1` | diagnóstico da cauda do h3 (§5.1); não entra nas estatísticas |

- **Falhas:** nenhuma das 900 repetições foi para o `failures.log`. A nova tentativa do `run.sh` resolveu a única falha de probe.
- **Integridade:** `dials = 1` e `recorder_stalls = 0` em todas.
- **Replicação:** as medianas das rodadas A e B batem em todos os cenários. Exemplos: p99 do s2-loss-5-low no h2, 310 contra 282 ms; p99 do s3 no h3, 417 contra 445 ms; razão de RPCs ok no s2, mesma ordem nas duas. A exceção é o s1 da rodada A, com ruído (§7).
- **Rodadas antigas** (`96fdb71`, 2026-10-01): mesmos resultados de s2 a 2000 RPC/s e s5, com 30 repetições. Ficam como histórico; o s3 daquela rodada é inválido (netem reordenando pacotes, corrigido em `daec7d1`).

## 2. Critérios de aceite da Fase 2 (spec §6.6)

| Critério | Resultado |
|---|---|
| A matriz roda sem intervenção | ✅ 900 de 900 repetições; 1 nova tentativa automática |
| Arquivos completos em cada repetição | ✅ |
| `make analyze` gera `summary.csv` e os gráficos | ✅, inclusive juntando rodadas |
| p99 do h2 cresce de forma monotônica de 1% → 5% | ✅ a 200 RPC/s, latência de transporte: 32 → 98 → 302 ms. (A 2000 RPC/s também cresce, mas é tempo de fila.) |
| s5 mostra queda e retomada, e calcula `recovery_s` | ✅ |
| `cv_p50 < 0,10` | ✅ s1 limpo (0,04 / 0,07), s2-low (≤ 0,02), s5 (0,006), s3 h2 (0,001) · ❌ s3 h3 (0,12): o h3 varia sob jitter, e isso é resultado, não ruído (§5) |

---

## 3. s1-baseline — rede ideal (80 repetições limpas)

Base: rodada B (30) + `2026-10-01-23-09-19` (50), mesmo commit, nenhuma repetição degradada. A rodada A ficou de fora por ruído de host (§7).

| Métrica | h2 | h3 | Δ (HL) [IC] | p (Holm) | δ |
|---|---|---|---|---|---|
| p50 | 0,815 ms | 0,793 ms | −0,022 [−0,031; −0,013] | 5·10⁻⁵ | −0,41 |
| p99 | 2,22 ms | 1,86 ms | −0,35 [−0,38; −0,32] | 2·10⁻¹⁶ | −0,78 |
| p99.9 | 2,98 ms | 3,02 ms | −0,02 [−0,15; +0,10] | 1,0 | −0,04 |
| handshake | 3,51 ms | 4,07 ms | +0,55 [+0,37; +0,73] | 3·10⁻⁷ | +0,50 |
| CPU do servidor por 1000 RPC/s | 42,3 | 47,0 | +5,0 [+4,3; +5,9] | 2·10⁻²¹ | +0,89 |
| CPU do loadgen (%) | 76 | 100 | — | — | — |

**Leitura:**
- **Latência:** o h3 tem cauda um pouco menor no p99; no p50 a diferença é de 22 µs, e no p99.9 não há diferença.
- **Handshake:** com RTT ≈ 0, mede CPU, e a inicialização do quic-go custa ~0,5 ms a mais.
- **CPU:** o QUIC em espaço de usuário custa 11% a mais no servidor e ~30% a mais no cliente.
- **Para o TCC (§4, Cenário 1):** com 80 repetições, o teste separa décimos de milissegundo, mas os efeitos são pequenos demais para importar na prática. A conclusão é equivalência, com o custo de CPU do QUIC como único efeito relevante.
- **Replicação:** o mesmo padrão apareceu nas rodadas de 30 (`96fdb71`) e de 50 repetições.

---

## 4. s2-loss — perda de pacotes (1%, 3%, 5% por direção, RTT de 10 ms; 50 repetições)

### 4.1 Latência sob perda — 200 RPC/s (`-low`)

Os dois transportes atendem **100% da demanda**, sem erros, em todos os níveis. A latência é de transporte, não de fila.

| Perda | Métrica | h2 | h3 | Δ (HL) [IC] | δ |
|---|---|---|---|---|---|
| 1% | p50 | 11,9 | 11,4 | −0,49 [−0,50; −0,47] | −1,00 |
| | p99 | 32,0 | 26,5 | −5,5 [−5,6; −5,4] | −1,00 |
| | p99.9 | 43,4 | 27,6 | −15,8 [−16,0; −15,5] | −1,00 |
| 3% | p50 | 17,1 | 11,4 | −5,7 [−5,8; −5,7] | −1,00 |
| | p99 | 98,3 | 27,3 | −71 [−106; −46] | −1,00 |
| | p99.9 | 252 | 42 | −210 [−212; −205] | −1,00 |
| 5% | p50 | 26,8 | 11,4 | −15,4 [−15,5; −15,2] | −1,00 |
| | p99 | 302 | 33 | −266 [−308; −239] | −0,96 |
| | p99.9 | 513 | 52 | −450 [−625; −350] | −0,96 |

Todos os p (Holm) < 10⁻¹⁴. Latências em ms.

**Leitura:**
- **O p50 do h3 não muda com a perda** (11,4 ms a 1%, 3% e 5%). No QUIC, uma perda atrasa só o stream do pacote perdido, e a maioria das RPCs nem nota.
- **No h2, toda a distribuição se desloca:** o p50 vai de 11,9 para 26,8 ms e o p99 de 32 para 302 ms. Cada retransmissão do TCP segura os ~200 streams em andamento.
- **Engasgos de vazão:** a 5%, as 50 repetições do h2 têm 1–8 segundos com vazão abaixo de 90% (`degraded_s`, mediana 4,5). No h3, só 2 repetições, com no máximo 2 s. São retransmissões segurando a conexão inteira por mais de um segundo.
- **CPU:** a 200 RPC/s, o custo por RPC é dominado por custos fixos, e as diferenças são pequenas e de sinal variável (h3 −10% a 1%, +14% a 5%).

### 4.2 Capacidade sob perda — 2000 RPC/s

A 2000 RPC/s, a demanda passa da capacidade de uma conexão sob perda: no h2 em todos os níveis, no h3 a partir de 3%. Aqui se compara **vazão**. A latência é tempo de fila (p50 de 5 a 38 s), não transporte.

| Perda | RPCs ok/s h2 | RPCs ok/s h3 | Razão h3/h2 [IC] | Demanda atendida h2 / h3 |
|---|---|---|---|---|
| 1% | 1.552 | 1.999 | 1,29× [1,28–1,31] | 78% / 100% |
| 3% | 660 | 1.676 | 2,55× [2,52–2,59] | 33% / 84% |
| 5% | 330 | 1.193 | 3,61× [3,54–3,68] | 17% / 60% |

- Todas com δ = 1 (separação total) e p (Holm) < 10⁻¹⁶.
- **Checagem de sanidade:** a fórmula de Mathis (vazão ≈ MSS/RTT × 1,22/√p) prevê ~1.600 RPC/s para o h2 a 1%, e o medido foi 1.552. A 3% e 5%, o medido fica abaixo da fórmula, porque os timeouts de retransmissão dominam e a fórmula não os modela.
- **No h3 a 1%**, o único caso sem saturação, a latência é de transporte: p50 de 28 ms e p99 de 188 ms.

**Por que o h3 aguenta mais** (hipóteses para o TCC):
1. **Sem head-of-line blocking entre streams** (a §4.1 mostra isso diretamente).
2. **Detecção de perda mais precisa:** números de pacote que nunca se repetem e ACKs com muitos intervalos.
3. **Controle de congestionamento diferente:** quic-go contra o do kernel.

**Handshake (todos os s2):** mediana de ~44 ms no h2 e ~34 ms no h3, Δ de −9,6 a −10,8 ms, ou seja, 1 RTT. Casos de 0,5–2,3 s (pacote do handshake perdido esperando o timeout inicial) aparecem mais no h2 (39 repetições) do que no h3 (14), e puxam as médias.

![CDF por cenário (rodada B)](../results/2026-10-07-06-59-47/analysis/latency_cdf.png)

*Rodada B (30 repetições). Nos painéis `-low`, a curva do h3 é quase a mesma nos três níveis, enquanto a do h2 se desloca para a direita com a perda. Nos painéis s2 a 2000 RPC/s, as curvas mostram tempo de fila.*

---

## 5. s3-jitter — 20 ms ± 10 ms (normal), 0% de perda, netem com `rate 10gbit` (50 repetições)

| Métrica | h2 | h3 | Δ (HL) [IC] | p (Holm) | δ |
|---|---|---|---|---|---|
| demanda atendida | 100% | 100% | — | 0,21 | −0,16 |
| p50 | 77,8 ms | 70,9 ms | −6,9 [−7,3; −6,4] | 2·10⁻¹² | −0,84 |
| p99 | 96 ms | 421 ms | +325 [+270; +481] | 2·10⁻¹⁴ | +0,92 |
| p99.9 | 102 ms | 551 ms | +448 [+379; +590] | 2·10⁻¹⁴ | +0,92 |
| handshake | 208 ms | 159 ms | −47 [−55; −39] | 7·10⁻¹³ | −0,86 |
| CPU do servidor por 1000 RPC/s | 9,0 | 9,0 | ≈ 0 | 0,74 | −0,04 |

**Leitura:**
- **Sem colapso:** com o netem corrigido, os dois atendem toda a demanda, e o cenário mede variação de atraso.
- **p50 de ~75 ms**, perto do dobro do RTT base de 40 ms. Numa fila FIFO com ~2000 pacotes/s, cada pacote espera o mais atrasado dos sorteios recentes, e o jitter vira atraso maior com saídas em rajadas ([`architecture.md` §5.2](architecture.md#52-injeção-de-falhas-netemsh)).
- **O h3 é melhor no caso típico e pior na cauda.** O p99 do h2 é estável (95–97 ms em todas as repetições). O do h3 vai de 91 ms a 2,5 s (quartis de 279 e 908 ms), e 35 das 50 repetições do h3 têm segundos de vazão abaixo de 90%; o h2 não tem nenhum.
- **Causa da cauda do h3:** perdas falsas que reduzem a janela de congestionamento (§5.1).
- **Handshake:** o h3 é ~47 ms mais rápido, cerca de 1 RTT nesse cenário.

### 5.1 Causa da cauda do h3 — diagnóstico com qlog

Uma repetição do s3 com `QLOG=1` (`2026-10-07-21-41-01`): qlog dos dois lados QUIC e contadores TCP do servidor (`nstat`). A latência dela reproduz o padrão das 50 repetições: p50 de 78 ms no h2 e 71 ms no h3; p99 de 97 ms no h2 e 321 ms no h3.

**O que o qlog mostra (conexão principal, 70 s):**

| | Servidor (envia as respostas) | Cliente (envia os pedidos) |
|---|---|---|
| pacotes enviados | 128.349 | 154.732 |
| perdas declaradas (`packet_lost`) | 28 (17 por limiar de reordenação, 11 por limiar de tempo) | 12 (por reordenação) |
| perdas falsas (`spurious_loss`) | 30 | 13 |
| entradas em recuperação (redução da janela) | 2, aos 24,7 s e 45,2 s | 1 |
| janela de congestionamento (mediana) | 174 KB | 120 KB |
| RTT (mediana / p99) | 62 / 80 ms | 61 / 80 ms |

Como o netem não perde pacotes no s3, **toda perda declarada é falsa**, e o próprio quic-go confirma isso com os eventos `spurious_loss`. Os pacotes "perdidos" chegaram depois de até 45 pacotes mais novos, ~85 ms atrasados.

**Como isso vira cauda:**
1. Aos 24,7 s, o servidor declara 20 pacotes perdidos de uma vez e entra em recuperação: a janela cai de ~200 KB para 145 KB (redução multiplicativa do Cubic, ×0,7).
2. 13 ms depois, o quic-go detecta que as perdas eram falsas, mas **não restaura a janela**. Ela volta a crescer devagar, ~8,6 KB a cada 0,7 s, e leva ~5 s para se recuperar.
3. Nesse período, a janela fica abaixo do produto banda × atraso da carga (~2,4 MB/s × 62 ms ≈ 150 KB). As respostas fazem fila no servidor, e o p50 sobe para 150–240 ms e o p99 para mais de 400 ms durante ~4 s.
4. O mesmo acontece aos 45,2 s. As 4.956 RPCs acima de 200 ms (4,1% do total, ou seja, toda a cauda do p99) caem nessas duas janelas. Fora delas, o h3 se comporta como o h2.

**O TCP no mesmo cenário:** o servidor h2 registrou 87 eventos de reordenação (`TCPSACKReorder`), mas retransmitiu só 5 segmentos em 221.756 e teve 0 RTOs falsos. O TCP do Linux usa RACK, que alarga a janela de tolerância à reordenação quando a detecta, e desfaz a redução da janela quando o receptor indica que a retransmissão foi desnecessária (DSACK).

**Conclusão para o TCC:** a cauda maior do h3 sob jitter é um comportamento da **implementação** (quic-go v0.63: detecção de perda menos tolerante à reordenação e sem desfazer a redução da janela após perda falsa), e não uma limitação do protocolo QUIC. O RFC 9002 permite adaptar o limiar de reordenação e desfazer a redução, mas não exige. A variabilidade entre repetições (p99 de 91 ms a 2,5 s) vem de quantas reduções caem em cada repetição.

**Ressalva:** mesmo com o netem em FIFO (`rate 10gbit`), ainda há alguma reordenação no caminho; os dois transportes a detectaram. A origem não foi identificada (candidatos: segmentação GSO e a interação do `rate` com o jitter). Ela afeta os dois transportes igualmente; só a reação de cada pilha difere.

---

## 6. s5-outage — apagão de 5 s no servidor (RTT de 10 ms; 50 repetições)

Todas as repetições estão limpas (`degraded_s = 0`) e sem saturação.

| Métrica | h2 | h3 | Δ (HL) [IC] | p (Holm) | δ |
|---|---|---|---|---|---|
| recovery_s | 1 s (50 × 1 s) | 4 s (39 × 4 s, 6 × 5 s, 4 × 1 s, 1 × 0 s) | +3 [+3; +3] | 10⁻¹⁶ | +0,88 |
| p50 | 11,09 ms | 11,06 ms | −0,013 [−0,039; +0,004] | 0,31 | −0,17 |
| p99 (backlog do apagão) | 5,54 s | 8,28 s | +2,74 s [2,73; 2,75] | 4·10⁻¹¹ | +0,80 |
| erros | 1,25% | 1,67% | +0,42 pp | 10⁻¹² | +0,80 |
| handshake | 43,7 ms | 34,2 ms | −9,6 [−9,7; −9,4] | 8·10⁻¹⁷ | −1,00 |
| CPU do servidor por 1000 RPC/s | 34,4 | 32,7 | −1,2 [−1,9; −0,4] | 0,005 | −0,37 |

**Leitura:**
- **Recuperação do h2:** determinística, 1 s. O próximo RTO do TCP dispara logo depois do fim do apagão.
- **Recuperação do h3:** 4–5 s em 90% das repetições. O h3 espera o próximo disparo do PTO, que dobra a cada tentativa sem resposta. Nas outras 10%, o disparo caiu logo depois do fim do apagão (0–1 s).
- **Efeitos da volta mais lenta:** mais RPCs estouram o prazo de 2 s (+0,4 pp) e o backlog cresce (p99 +2,7 s). No s5, o p99 mede o backlog, não o transporte ([`architecture.md` §6.4](architecture.md#64-tempo-de-recuperação-s5)).
- **Fora do apagão:** o p50 empata. O h3 gasta 5% menos CPU por RPC, um efeito pequeno.
- **Handshake:** 3 RTT no h2 contra 2 no h3. A diferença esperada era de ~10 ms, e o medido foi 9,6 ms.

![s5 vazão](../results/pooled-2026-10-07/s5_throughput.png)

*s5 (50 repetições por transporte): todas as linhas zeram no apagão. O h2 volta 1 s depois do fim, com o pico de backlog; a maioria do h3, 4–5 s depois.*

---

## 7. Validade — o que pode enganar

| Problema | Onde | Efeito | Tratamento |
|---|---|---|---|
| Ruído de CPU no host no início da rodada | s1 da rodada A (repetições 2–10, ~25 min) e também na rodada de validação de 2026-10-01 | CPU do servidor 20–40% maior e p50 até 2× maior; o `degraded_s` pegou só 1 das 9 repetições, porque a vazão se manteve | s1 tirado das rodadas limpas (§3); antes de rodar, esperar o host assentar (`vmstat 1`) ou descartar os primeiros minutos |
| Saturação | s2 a 2000 RPC/s | latência vira tempo de fila; `p*_all` sai abaixo do `p*` | comparar vazão; latência só nos `-low` |
| Falha do probe sob perda | s2-loss-3-low (1 em 900) | sem efeito: a nova tentativa automática resolveu | — |
| Significância × relevância | s1 | com 80 repetições, diferenças de µs ficam significativas | discutir o tamanho do efeito (HL, δ), não só o p |
| CPU a 200 RPC/s | s2-low | custo por RPC dominado por custos fixos | comparar CPU só a 2000 RPC/s |
| Reordenação residual no netem | s3 | mesmo com `rate`, há pacotes chegando até ~85 ms fora de ordem | afeta os dois transportes; a diferença está na reação de cada pilha (§5.1) |
| Custo do qlog | diagnóstico (`QLOG=1`) | CPU extra e ~140 MB por lado por repetição | usar só para diagnóstico, nunca nas rodadas do TCC |
| Uma VM, um host | todos | os valores absolutos dependem do hardware | as comparações h2 × h3 são relativas e pareadas no tempo (h2 e h3 alternados) |

---

## 8. Próximos passos

1. **s3 — causa confirmada (§5.1).** Opcional:
   - investigar a origem da reordenação residual no netem (por exemplo, com `tc -s qdisc` e captura de pacotes numa repetição);
   - rodar o s3 com jitter menor (±5 ms) para ver se a cauda do h3 some;
   - verificar se versões mais novas do quic-go desfazem a redução da janela após perda falsa.
2. **s5 — descrever a distribuição no TCC:** 1 s × 4–5 s, com a moda rápida do h3 em 10%. Não reportar só a média.
3. **Harness:** aquecer o host antes da matriz, por exemplo com uma repetição de descarte, ou acrescentar um detector de ruído por `cpu_per_krps`. O `degraded_s` não pega contenção de CPU que não derruba a vazão.
4. **Texto do TCC:**
   - Nas tabelas, usar mediana, HL com IC e δ; para vazão, a razão h3/h2 com IC. O p corrigido por Holm entra só como confirmação.
   - Gráficos: o CDF da rodada B e o `s5_throughput` do pooled.

---

### Apêndice — gráficos

Os PNGs ficam em `results/<pasta>/analysis/` (ou `results/pooled-2026-10-07/`); essas pastas não são versionadas.

| Conjunto | CDF | Boxplot p99 | Vazão s5 |
|---|---|---|---|
| pooled A + B (50 reps) | [latency_cdf](../results/pooled-2026-10-07/latency_cdf.png) ⚠ s1 inclui o ruído da rodada A | [p99_boxplot](../results/pooled-2026-10-07/p99_boxplot.png) | [s5_throughput](../results/pooled-2026-10-07/s5_throughput.png) |
| rodada B (30 reps) | [latency_cdf](../results/2026-10-07-06-59-47/analysis/latency_cdf.png) | [p99_boxplot](../results/2026-10-07-06-59-47/analysis/p99_boxplot.png) | [s5_throughput](../results/2026-10-07-06-59-47/analysis/s5_throughput.png) |
| rodada A (20 reps) | [latency_cdf](../results/2026-10-06-23-17-30/analysis/latency_cdf.png) | [p99_boxplot](../results/2026-10-06-23-17-30/analysis/p99_boxplot.png) | [s5_throughput](../results/2026-10-06-23-17-30/analysis/s5_throughput.png) |
| s1 com 50 reps | [latency_cdf](../results/2026-10-01-23-09-19/analysis/latency_cdf.png) | [p99_boxplot](../results/2026-10-01-23-09-19/analysis/p99_boxplot.png) | — |
