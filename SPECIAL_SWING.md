# Special swing

`special_swing.py` keeps the value swing flow unchanged and builds a separate
Top100 dataset for Codex App or Codex CLI analysis.

```powershell
python special_swing.py
```

Modes:

```powershell
python special_swing.py --swing-mode position  # 3~5 trading-day special swing
python special_swing.py --swing-mode day       # one-day morning entry / afternoon exit
python special_swing.py --swing-mode all       # generate both datasets/prompts
```

Defaults:

```text
Default mode: position
Technical pre-news pool: 100 stocks
Phase 2 AI shortlist target: 30 stocks
Phase 3 final debate target: 10 stocks
News: latest 50 items per stock from Naver
News analysis window: most recent 5 calendar dates ending at 08:00 KST
Day swing: Top100 -> Top20 -> Top5, post-close-to-08:00 KST news
News time budget: 180 seconds
AI mode: raw dataset and prompt generation only; no API key and no direct API scoring
```

Result files:

```text
data/results/YYYY-MM-DD_special_swing_all_evaluated.csv
data/results/YYYY-MM-DD_special_swing_candidates_top100.csv
data/results/YYYY-MM-DD_special_swing_news_raw_top100.md
data/results/YYYY-MM-DD_special_swing_news_dataset_top100.json
data/results/YYYY-MM-DD_special_swing_phase2_score_top100_to_top30_prompt.md
data/results/YYYY-MM-DD_special_swing_phase3_debate_top30_to_top10_prompt.md
data/results/YYYY-MM-DD_day_swing_all_evaluated.csv
data/results/YYYY-MM-DD_day_swing_candidates_top100.csv
data/results/YYYY-MM-DD_day_swing_news_raw_top100.md
data/results/YYYY-MM-DD_day_swing_news_dataset_top100.json
data/results/YYYY-MM-DD_day_swing_phase2_score_top100_to_top20_prompt.md
data/results/YYYY-MM-DD_day_swing_phase3_debate_top20_to_top5_prompt.md
```

The first pass filters for a 20-day box range, pullback from the 20-day high,
steady liquidity, and non-overheated price action. It also adds a 5-day trigger
score for volatility contraction, volume dry-up, and MA5/MA10/VWAP20 reclaim
setups. The technical score also includes community-style setup signals:
VCP tightening, Pocket Pivot volume, Anchored VWAP reclaim, relative strength
versus the market, and tight-base compression.

The script clears `data/results` before writing fresh output. It first saves an
all-stock audit CSV with eligibility and filter reasons, then collects recent
raw news for the full Top100 pool through the 08:00 KST cutoff and keeps
titles, preview descriptions, original links, Naver links, publish times, and
rule-based helper scores. Codex should read the generated Phase 2 prompt to
score all 100 candidates and select Top30, then read the Phase 3 prompt to run a
four-specialist plus leader debate and select final Top10.

For `--swing-mode day`, the script keeps the same raw-data-first workflow but
uses one-day scoring and filenames. It is a research briefing for morning entry
and afternoon exit, not an intraday auto-trading engine. Codex should select
Top20 in Phase 2 and final Top5 in Phase 3, and every final pick needs morning
entry, invalidation, afternoon exit, and no-trade conditions.
