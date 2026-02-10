# Flow Map Summary

Generated: 2026-02-10T16:15:39.743Z
Project: TUT_VARIABLES

## Quick Read
- Nodes: 35
- Edges: 34
- Recipes: 16
- Datasets: 16
- Roots: 5
- Leaves: 9
- Warnings: 0

## Recipe Types
| Type | Count |
| --- | ---: |
| python | 5 |
| shaker | 2 |
| join | 2 |
| implicit | 2 |
| distinct | 1 |
| pivot | 1 |
| topn | 1 |
| window | 1 |
| download | 1 |

## Edges
- cards_prepared -> compute_tx_joined (reads)
- cards -> compute_cards_analysis (reads)
- cards -> compute_cards_prepared (reads)
- compute_ass -> ass (writes)
- compute_cards_analysis -> cards_analysis (writes)
- compute_cards_prepared -> cards_prepared (writes)
- compute_tx_distinct -> tx_distinct (writes)
- compute_tx_joined -> tx_joined (writes)
- compute_tx_pivot -> tx_pivot (writes)
- compute_tx_prepared -> tx_prepared (writes)
- compute_tx_topn -> tx_topn (writes)
- compute_tx_windows -> tx_windows (writes)
- download_to_merchants -> jqBwVuQl (writes)
- eB0k3any -> FilesInFolder->TUT_VARIABLES.tx (reads)
- eB0k3any -> python_read_tx_2017 (reads)
- eB0k3any -> python_read_tx_2018 (reads)
- FilesInFolder->TUT_VARIABLES.merchants -> merchants (writes)
- FilesInFolder->TUT_VARIABLES.tx -> tx (writes)
- join_merchants_tx_joined -> merchants_tx_joined (writes)
- jqBwVuQl -> FilesInFolder->TUT_VARIABLES.merchants (reads)
- merchants -> compute_tx_joined (reads)
- merchants -> join_merchants_tx_joined (reads)
- python_combine_tx -> tx_combined (writes)
- python_read_tx_2017 -> tx_2017 (writes)
- python_read_tx_2018 -> tx_2018 (writes)
- tx_2017 -> python_combine_tx (reads)
- tx_2018 -> python_combine_tx (reads)
- tx_joined -> compute_tx_prepared (reads)
- tx_prepared -> compute_tx_distinct (reads)
- tx_prepared -> compute_tx_pivot (reads)
- tx_prepared -> compute_tx_topn (reads)
- tx_prepared -> compute_tx_windows (reads)
- tx -> compute_tx_joined (reads)
- tx -> join_merchants_tx_joined (reads)

## Recipe Pipelines
- compute_ass (python): (none) -> ass
- compute_cards_analysis (python): cards -> cards_analysis
- compute_cards_prepared (shaker): cards -> cards_prepared
- compute_tx_distinct (distinct): tx_prepared -> tx_distinct
- compute_tx_joined (join): cards_prepared, merchants, tx -> tx_joined
- compute_tx_pivot (pivot): tx_prepared -> tx_pivot
- compute_tx_prepared (shaker): tx_joined -> tx_prepared
- compute_tx_topn (topn): tx_prepared -> tx_topn
- compute_tx_windows (window): tx_prepared -> tx_windows
- download_to_merchants (download): (none) -> jqBwVuQl
- FilesInFolder->TUT_VARIABLES.merchants (implicit): jqBwVuQl -> merchants
- FilesInFolder->TUT_VARIABLES.tx (implicit): eB0k3any -> tx
- join_merchants_tx_joined (join): merchants, tx -> merchants_tx_joined
- python_combine_tx (python): tx_2017, tx_2018 -> tx_combined
- python_read_tx_2017 (python): eB0k3any -> tx_2017
- python_read_tx_2018 (python): eB0k3any -> tx_2018

## Mermaid
```mermaid
graph LR
  n0["ass  (dataset)"]
  n1["cards  (dataset)"]
  n2["cards_analysis  (dataset)"]
  n3["cards_prepared  (dataset)"]
  n4["compute_ass  (recipe:python)"]
  n5["compute_cards_analysis  (recipe:python)"]
  n6["compute_cards_prepared  (recipe:shaker)"]
  n7["compute_tx_distinct  (recipe:distinct)"]
  n8["compute_tx_joined  (recipe:join)"]
  n9["compute_tx_pivot  (recipe:pivot)"]
  n10["compute_tx_prepared  (recipe:shaker)"]
  n11["compute_tx_topn  (recipe:topn)"]
  n12["compute_tx_windows  (recipe:window)"]
  n13["download_to_merchants  (recipe:download)"]
  n14["eB0k3any  (folder)"]
  n15["FilesInFolder->TUT_VARIABLES.merchants  (recipe:implicit)"]
  n16["FilesInFolder->TUT_VARIABLES.tx  (recipe:implicit)"]
  n17["join_merchants_tx_joined  (recipe:join)"]
  n18["jqBwVuQl  (folder)"]
  n19["merchants  (dataset)"]
  n20["merchants_tx_joined  (dataset)"]
  n21["python_combine_tx  (recipe:python)"]
  n22["python_read_tx_2017  (recipe:python)"]
  n23["python_read_tx_2018  (recipe:python)"]
  n24["qRd6Z2aM  (folder)"]
  n25["tx  (dataset)"]
  n26["tx_2017  (dataset)"]
  n27["tx_2018  (dataset)"]
  n28["tx_combined  (dataset)"]
  n29["tx_distinct  (dataset)"]
  n30["tx_joined  (dataset)"]
  n31["tx_pivot  (dataset)"]
  n32["tx_prepared  (dataset)"]
  n33["tx_topn  (dataset)"]
  n34["tx_windows  (dataset)"]
  n1 -->|reads| n5
  n1 -->|reads| n6
  n3 -->|reads| n8
  n4 -->|writes| n0
  n5 -->|writes| n2
  n6 -->|writes| n3
  n7 -->|writes| n29
  n8 -->|writes| n30
  n9 -->|writes| n31
  n10 -->|writes| n32
  n11 -->|writes| n33
  n12 -->|writes| n34
  n13 -->|writes| n18
  n14 -->|reads| n16
  n14 -->|reads| n22
  n14 -->|reads| n23
  n15 -->|writes| n19
  n16 -->|writes| n25
  n17 -->|writes| n20
  n18 -->|reads| n15
  n19 -->|reads| n8
  n19 -->|reads| n17
  n21 -->|writes| n28
  n22 -->|writes| n26
  n23 -->|writes| n27
  n25 -->|reads| n8
  n25 -->|reads| n17
  n26 -->|reads| n21
  n27 -->|reads| n21
  n30 -->|reads| n10
  n32 -->|reads| n7
  n32 -->|reads| n9
  n32 -->|reads| n11
  n32 -->|reads| n12
```

