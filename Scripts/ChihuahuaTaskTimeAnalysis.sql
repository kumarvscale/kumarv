Select
  ROUND(ta.v2_time_spent_secs/60,2) as v2_time_spent_mins,
  ROUND(ta.v2_active_time_spent_secs/60,2) as v2_active_time_spent_mins,
  cast(ta.attempted_at as Date) as attempted_at,
  pj.Name,
  v3.review_level,
  ta._ID,
  ta.Task,
  ta.attempted_by
from
  public.taskattempts ta
  left join PIPELINEV3HUMANNODES v3 on ta.task=v3.task 
  left join projects pj on ta.project=pj._ID
where
  ta.project in (
    '64de664d426fb9f03c83e73d',
    '64de6687d1f6b586efeb1596',
    '64de63dbe1df5ee41eb0bdc5',
    '64de671505668ea875fa5fe0',
    '64de672c93ad3f1536108882',
    '64de67aef168bd56beb6b1d7',
    '64de62829672f1286b545cc0',
    '64de66afc83a82961258add5',
    '64de642d4294b1b0d5af25f2',
    '64de6516ea56fa4cb04ba160',
    '64dd840356ab663fcd219d99',
    '64de6755d6a2362a21ae97f9',
    '64de66eb4f5c495d231a2eb8',
    '64de6587a1472e3a571020df',
    '64de68d37e4ee9c5482f7d0a',
    '64de684c2fb5611b24ccacc3',
    '64de688b102709fde54a6a21',
    '64de68117063e52806e3dfb5',
    '64de65d3a6e7fd7726c4ab40',
    '64de65a2ea56fa4cb04bc328',
    '64de654d92add42bcfeeb0e9',
    '64de648e0d52011b4702b175',
    '64de64744d0f3ac2d028a0a7',
    '64de660e1c964afee02f261f',
    '64de64ff3155c680b93549c0',
    '64de64c40948dc7f0d98b75b',
    '64d3b33e2bc9a62e70ecce39'
  ) limit 10