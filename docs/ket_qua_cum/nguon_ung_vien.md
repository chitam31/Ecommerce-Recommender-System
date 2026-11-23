[ung_vien] mô hình als_v2_r20261110T230924 (rank 50, reg 1.0, alpha 40.0), features v3
[ung_vien] 9467 user, 8673 user có ứng viên đồng xuất hiện (5.1 phút)
[ung_vien] q_c=0 q_d=0 val=0.10122 kiem_tra=0.07417 ca_thang=0.08647 coverage=0.0647
[ung_vien] q_c=1 q_d=0 val=0.10222 kiem_tra=0.07534 ca_thang=0.08763 coverage=0.1014
[ung_vien] q_c=2 q_d=0 val=0.10335 kiem_tra=0.07600 ca_thang=0.08864 coverage=0.1268
[ung_vien] q_c=3 q_d=0 val=0.10401 kiem_tra=0.07640 ca_thang=0.08919 coverage=0.1453
[ung_vien] q_c=0 q_d=1 val=0.10095 kiem_tra=0.07404 ca_thang=0.08630 coverage=0.0644
[ung_vien] q_c=0 q_d=2 val=0.10096 kiem_tra=0.07372 ca_thang=0.08626 coverage=0.0646
[ung_vien] q_c=1 q_d=1 val=0.10244 kiem_tra=0.07512 ca_thang=0.08772 coverage=0.1009
[ung_vien] q_c=2 q_d=1 val=0.10336 kiem_tra=0.07618 ca_thang=0.08870 coverage=0.1263
[ung_vien] q_c=3 q_d=1 val=0.10405 kiem_tra=0.07630 ca_thang=0.08916 coverage=0.1444
[ung_vien] q_c=2 q_d=2 val=0.10328 kiem_tra=0.07558 ca_thang=0.08841 coverage=0.1259

## Thêm nguồn ứng viên ngoài ALS — mô hình `als_v2_r20261110T230924`, features v3, xếp lại β = 1.0 trên 200 ứng viên

Mỗi cấu hình (q_c, q_d) giữ 10 − q_c − q_d ô đầu của danh sách đang phục vụ, thêm q_c món đồng xuất hiện và q_d món bán chạy theo danh mục. Coverage trên 126,230 item của train.

| q_c | q_d | NDCG@10 validation | NDCG@10 kiểm tra | NDCG@10 cả tháng | Coverage cả tháng | Số item | Suất vào N1 | Novelty |
|---|---|---|---|---|---|---|---|---|
| 0 | 0 | 0.1012 | 0.0742 | 0.0865 | 6.47% | 8,162 | 80.8% | 12.17 |
| 1 | 0 | 0.1022 | 0.0753 | 0.0876 | 10.14% | 12,805 | 78.2% | 12.29 |
| 2 | 0 | 0.1034 | 0.0760 | 0.0886 | 12.68% | 16,011 | 75.9% | 12.39 |
| 3 | 0 | 0.1040 **(chọn)** | 0.0764 | 0.0892 | 14.53% | 18,341 | 74.1% | 12.48 |
| 0 | 1 | 0.1009 | 0.0740 | 0.0863 | 6.44% | 8,124 | 82.2% | 12.06 |
| 0 | 2 | 0.1010 | 0.0737 | 0.0863 | 6.46% | 8,151 | 83.3% | 11.98 |
| 1 | 1 | 0.1024 | 0.0751 | 0.0877 | 10.09% | 12,737 | 79.5% | 12.18 |
| 2 | 1 | 0.1034 | 0.0762 | 0.0887 | 12.63% | 15,939 | 77.4% | 12.28 |
| 3 | 1 | 0.1041 | 0.0763 | 0.0892 | 14.44% | 18,231 | 75.6% | 12.37 |
| 2 | 2 | 0.1033 | 0.0756 | 0.0884 | 12.59% | 15,891 | 78.6% | 12.20 |

Baseline toàn sàn: NDCG@10 cả tháng 0.0051, kỳ kiểm tra 0.0049. Luật chọn: coverage cao nhất trong các cấu hình có NDCG@10 validation ≥ 95 % cấu hình (0, 0).

```json
{
 "mo_hinh": "als_v2_r20261110T230924",
 "phien_ban_features": 3,
 "beta": 1.0,
 "so_item_train": 126230,
 "so_user_ca_thang": 9467,
 "so_user_val": 6505,
 "so_user_kiem_tra": 4215,
 "user_co_dong_xuat_hien": 8673,
 "baseline_toan_san": {
  "ca_thang": 0.005086301556620911,
  "kiem_tra": 0.004881371831176868
 },
 "goc": {
  "q_c": 0,
  "q_d": 0,
  "ndcg_val": 0.10122244999694806,
  "coverage_val": 0.05943911906836727,
  "ndcg_kiem_tra": 0.07417297849632949,
  "ndcg_ca_thang": 0.08647176909370621,
  "coverage_ca_thang": 0.0646597480789036,
  "so_item_ca_thang": 8162,
  "novelty_ca_thang": 12.172632488154226,
  "ty_le_n1": 0.808
 },
 "chon": {
  "q_c": 3,
  "q_d": 0,
  "ndcg_val": 0.10401239824080259,
  "coverage_val": 0.12218173175948666,
  "ndcg_kiem_tra": 0.07639993882145213,
  "ndcg_ca_thang": 0.08918666927914116,
  "coverage_ca_thang": 0.14529826507169452,
  "so_item_ca_thang": 18341,
  "novelty_ca_thang": 12.479930287946173,
  "ty_le_n1": 0.7411
 },
 "dat_nguong_coverage": true,
 "uplift_ca_thang_so_voi_baseline": 16.53467982311147,
 "cac_cau_hinh": [
  {
   "q_c": 0,
   "q_d": 0,
   "ndcg_val": 0.10122244999694806,
   "coverage_val": 0.05943911906836727,
   "ndcg_kiem_tra": 0.07417297849632949,
   "ndcg_ca_thang": 0.08647176909370621,
   "coverage_ca_thang": 0.0646597480789036,
   "so_item_ca_thang": 8162,
   "novelty_ca_thang": 12.172632488154226,
   "ty_le_n1": 0.808
  },
  {
   "q_c": 1,
   "q_d": 0,
   "ndcg_val": 0.1022209714336433,
   "coverage_val": 0.08684147983839025,
   "ndcg_kiem_tra": 0.07533862035544228,
   "ndcg_ca_thang": 0.08762508093468938,
   "coverage_ca_thang": 0.10144181256436663,
   "so_item_ca_thang": 12805,
   "novelty_ca_thang": 12.291863693063027,
   "ty_le_n1": 0.7816
  },
  {
   "q_c": 2,
   "q_d": 0,
   "ndcg_val": 0.10335498389232642,
   "coverage_val": 0.10693971322189653,
   "ndcg_kiem_tra": 0.07599519756795343,
   "ndcg_ca_thang": 0.08864400978398476,
   "coverage_ca_thang": 0.12683989542897886,
   "so_item_ca_thang": 16011,
   "novelty_ca_thang": 12.39344087190918,
   "ty_le_n1": 0.7595
  },
  {
   "q_c": 3,
   "q_d": 0,
   "ndcg_val": 0.10401239824080259,
   "coverage_val": 0.12218173175948666,
   "ndcg_kiem_tra": 0.07639993882145213,
   "ndcg_ca_thang": 0.08918666927914116,
   "coverage_ca_thang": 0.14529826507169452,
   "so_item_ca_thang": 18341,
   "novelty_ca_thang": 12.479930287946173,
   "ty_le_n1": 0.7411
  },
  {
   "q_c": 0,
   "q_d": 1,
   "ndcg_val": 0.10094700078331753,
   "coverage_val": 0.059027172621405374,
   "ndcg_kiem_tra": 0.07404408198414145,
   "ndcg_ca_thang": 0.08629594556920127,
   "coverage_ca_thang": 0.06435871029073913,
   "so_item_ca_thang": 8124,
   "novelty_ca_thang": 12.057253259478786,
   "ty_le_n1": 0.8221
  },
  {
   "q_c": 0,
   "q_d": 2,
   "ndcg_val": 0.10095840588134358,
   "coverage_val": 0.058781589162639626,
   "ndcg_kiem_tra": 0.07372408785218161,
   "ndcg_ca_thang": 0.08626370356007124,
   "coverage_ca_thang": 0.06457260556127703,
   "so_item_ca_thang": 8151,
   "novelty_ca_thang": 11.978923889001168,
   "ty_le_n1": 0.833
  },
  {
   "q_c": 1,
   "q_d": 1,
   "ndcg_val": 0.10244219719493454,
   "coverage_val": 0.08614433969737781,
   "ndcg_kiem_tra": 0.07511667811765337,
   "ndcg_ca_thang": 0.08771908979535431,
   "coverage_ca_thang": 0.10090311336449338,
   "so_item_ca_thang": 12737,
   "novelty_ca_thang": 12.177624600841689,
   "ty_le_n1": 0.7954
  },
  {
   "q_c": 2,
   "q_d": 1,
   "ndcg_val": 0.10335616346818507,
   "coverage_val": 0.10615543056325755,
   "ndcg_kiem_tra": 0.07617546463618237,
   "ndcg_ca_thang": 0.0886959621901568,
   "coverage_ca_thang": 0.12626950804087778,
   "so_item_ca_thang": 15939,
   "novelty_ca_thang": 12.278252306554307,
   "ty_le_n1": 0.7738
  },
  {
   "q_c": 3,
   "q_d": 1,
   "ndcg_val": 0.10405248655386395,
   "coverage_val": 0.12131822863027807,
   "ndcg_kiem_tra": 0.07629715994601284,
   "ndcg_ca_thang": 0.08916487959758126,
   "coverage_ca_thang": 0.14442683989542898,
   "so_item_ca_thang": 18231,
   "novelty_ca_thang": 12.365862375151972,
   "ty_le_n1": 0.756
  },
  {
   "q_c": 2,
   "q_d": 2,
   "ndcg_val": 0.10327911411829621,
   "coverage_val": 0.10551374475164382,
   "ndcg_kiem_tra": 0.0755831669496521,
   "ndcg_ca_thang": 0.08841107649315139,
   "coverage_ca_thang": 0.12588924978214372,
   "so_item_ca_thang": 15891,
   "novelty_ca_thang": 12.200274175219763,
   "ty_le_n1": 0.7857
  }
 ],
 "so_phut": 11.2
}
```
