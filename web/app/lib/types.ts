export type SpecialSwingPick = {
  code: string;
  entry_price?: string | number;
  final_rank: number;
  full_take_profit_price?: string | number;
  grade?: string;
  half_take_profit_price?: string | number;
  key_catalyst?: string;
  key_news_links?: string;
  key_risk?: string;
  leader_score?: string | number;
  name: string;
  reason?: string;
  risk?: string;
};

export type SpecialSwingSummary = {
  generated_at: string;
  id: string;
  signal_date: string;
  title: string;
  top5: SpecialSwingPick[];
  type: "special_swing";
  url?: string;
};

export type SpecialSwingResult = SpecialSwingSummary & {
  detail_markdown: string;
  files?: Record<string, string>;
  telegram_brief: string;
  top10: SpecialSwingPick[];
};

export type SpecialSwingIndex = {
  items: SpecialSwingSummary[];
  updated_at: string;
};
