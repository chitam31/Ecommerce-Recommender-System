"""Chọn chiến lược gợi ý cho một lượt hiển thị trang chủ.

Thứ tự ưu tiên:
  1. Có danh sách trong gold.goi_y_nguoi_dung (job batch đã trộn sẵn theo nhóm A/B)
     -> thay các ô thịnh hành bằng dữ liệu mới nhất của luồng.
  2. Không có trong gold nhưng phiên hiện tại có tương tác -> bán chạy theo danh mục
     vừa xem, xen kẽ 50/50 thịnh hành (chiến lược "< 5 tương tác").
  3. Không có gì -> user mới hoàn toàn: top thịnh hành 15 phút gần nhất.
"""
from common.tron import (
    NGUON_ALS,
    chon_nhom,
    danh_sach_it_tuong_tac,
    danh_sach_nguoi_moi,
    thay_o_thinh_hanh,
)

SO_HIEN_THI = 10


def tron_danh_muc(cac_ds):
    """Xen kẽ các danh sách bán chạy: món 1 của mỗi danh mục, rồi món 2..."""
    dai_nhat = max([len(d) for d in cac_ds] or [0])
    return [ds[i] for i in range(dai_nhat) for ds in cac_ds if i < len(ds)]


def goi_y_trang_chu(ma_nguoi_dung, kho, phien):
    """Trả về dict: nhom_ab, chien_luoc, goi_y (10 món), thinh_hanh (10 món)."""
    nhom = chon_nhom(ma_nguoi_dung)
    thinh_hanh = kho.thinh_hanh(30)
    ds_gold = kho.goi_y_cua(ma_nguoi_dung)

    if ds_gold:
        chien_luoc = "als" if any(g["nguon"] == NGUON_ALS for g in ds_gold) else "it_tuong_tac"
        ds = thay_o_thinh_hanh(ds_gold, thinh_hanh)
    else:
        cac_dm = phien.danh_muc_da_xem(ma_nguoi_dung)
        if cac_dm:
            chien_luoc = "it_tuong_tac"
            ds = danh_sach_it_tuong_tac(tron_danh_muc([kho.ban_chay_danh_muc(dm) for dm in cac_dm]), thinh_hanh)
        else:
            chien_luoc = "nguoi_moi"
            ds = danh_sach_nguoi_moi(thinh_hanh)

    return {
        "ma_nguoi_dung": ma_nguoi_dung,
        "nhom_ab": nhom,
        "chien_luoc": chien_luoc,
        "goi_y": ds[:SO_HIEN_THI],
        "thinh_hanh": thinh_hanh[:SO_HIEN_THI],
        "phien_ban_mo_hinh": ds_gold[0].get("phien_ban_mo_hinh") if ds_gold else None,
    }
