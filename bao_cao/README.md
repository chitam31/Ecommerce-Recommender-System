# Báo cáo cuối kỳ

Báo cáo và slide có hai ngôn ngữ, cùng số liệu. Báo cáo dựng ban đầu bằng `tao_bao_cao.js`, bản nộp được hoàn thiện trong Word rồi xuất PDF bằng `cap_nhat_muc_luc.py` (ảnh giữ nguyên độ phân giải); slide dựng bằng `slide/gen.py` (tiếng Anh) và `slide/gen_vi.py` (tiếng Việt), 28 slide:

| Bản | Tệp | Dùng để |
|---|---|---|
| **Báo cáo chính, tiếng Việt** (cấu trúc C.3, thân bài 35 trang, tổng 40 trang) | `CS411V_REPORT Final Project_Group Chí Tâm & Quốc Anh_Viet.pdf`| **Bản nộp** |
| Báo cáo chính, tiếng Anh (dịch đầy đủ bản trên, cùng bố cục, tổng 36 trang) | `CS411V_REPORT Final Project_Group Chí Tâm & Quốc Anh_Eng.pdf` | Bản tiếng Anh của bản nộp |


Hình và biểu đồ ghi nhãn tiếng Anh, dùng chung cho hai ngôn ngữ (chú thích hình theo ngôn ngữ của bản); riêng `c08_eda`, `c09_phan_tich_loi` có hai bản nhãn (`*_en` cho bản tiếng Anh). Ba sơ đồ do nhóm thiết kế sau góp ý của GVHD ngày 19/11 được dựng bằng HTML + SVG vector trong `slide/so_do.py`, có nhãn riêng cho từng ngôn ngữ: kiến trúc tổng thể và vai trò các công nghệ (slide 6, Hình 2.1), Airflow điều phối năm DAG (slide 8, Hình 2.2), lakehouse với luồng lưu trữ và luồng tính toán (slide 10, Hình 4.1). `hinh/xuat_so_do.py` chụp ba sơ đồ này thành `hinh/so_do_*_{vi,en}.png` (khoảng 3.900 px) cho báo cáo. Phần kiến trúc (slide 5–11) theo thứ tự: cụm sáu máy, kiến trúc tổng thể, ba job luồng, năm DAG, vòng khép kín, lakehouse, DAG 1 chi tiết. Báo cáo có thêm các hình của slide: Hình 2.3, 5.1, 5.2, 6.2–6.8, 8.1, 8.2.


## Dựng lại

```bash
# Hình và biểu đồ
cd bao_cao/hinh
for f in f0*.py f1*.py; do python3 "$f"; done
dot -Tpng -Gdpi=220 f04_dong_du_lieu.dot -o f04_dong_du_lieu.png
dot -Tpng -Gdpi=220 f05_dag1.dot -o f05_dag1.png
# f06_dag_khac.png: vẽ riêng từng cluster của f06_dag_khac.dot rồi xếp chồng dọc bằng PIL
python3 bieu_do.py && python3 bieu_do_cum.py && python3 cong_thuc.py
python3 xuat_so_do.py                  # ba sơ đồ nhóm thiết kế -> so_do_*_{vi,en}.png cho báo cáo

# Slide (28 trang): sinh HTML từ gen_vi.py / gen.py rồi xuất PDF vector
cd ../slide
python3 gen_vi.py && python3 gen.py
python3 dung_thu_slide.py --vi --pdf "../CS411V_SLIDE Final Project_Group Chí Tâm & Quốc Anh_Viet.pdf" \
  $(python3 -c "import json;print(' '.join(json.load(open('project_vi/deck.json'))['order']))")
python3 dung_thu_slide.py --pdf "../CS411V_SLIDE Final Project_Group Chí Tâm & Quốc Anh_Eng.pdf" \
  $(python3 -c "import json;print(' '.join(json.load(open('project/deck.json'))['order']))")

# Báo cáo: bản nộp được hoàn thiện trong Word (tệp .docx không đưa lên repo); từ tệp .docx đó,
# cập nhật mục lục và xuất PDF cùng tên, ảnh giữ nguyên độ phân giải:
cd ..
python3 cap_nhat_muc_luc.py "<tệp báo cáo>.docx"
```

`tao_bao_cao.js` là công cụ dựng bản thảo ban đầu của báo cáo; nguồn bản thảo (`noi_dung*/`) đã được gỡ khỏi repo
sau khi báo cáo được hoàn thiện trong Word, nên tệp này chỉ còn để tham khảo.

Khi mở `.docx` trong Word, chọn **Yes** ở hộp thoại cập nhật trường để Word điền mục lục,
danh mục hình và danh mục bảng.
