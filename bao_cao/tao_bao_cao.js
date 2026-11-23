// tao_bao_cao.js - dựng báo cáo Word từ các tệp Markdown rút gọn trong noi_dung/.
//
//   node tao_bao_cao.js            -> BD03_Recommender_System_Report.docx   (tiếng Anh, noi_dung/)
//   node tao_bao_cao.js vi         -> BD03_Bao_cao_He_thong_goi_y.docx       (tiếng Việt, noi_dung_vi/)
//   node tao_bao_cao.js chinh      -> BD03_Bao_cao_chinh.docx                (bản chính 15–20 trang theo mục C.3
//                                                                            của đề cương, noi_dung_chinh/)
//   node tao_bao_cao.js chinh en   -> BD03_Main_Report_EN.docx               (bản chính tiếng Anh, noi_dung_chinh_en/)
//
// Cú pháp hỗ trợ (đủ cho báo cáo, không phải Markdown đầy đủ):
//   # Title {#sec:id}            chương (tự đánh số 1, 2, ...; "# !Title" không đánh số; "# @A Title" phụ lục A)
//   ## / ### / ####              mục con (#### không đánh số)
//   đoạn văn: các dòng liền nhau; **đậm**, *nghiêng*, `mã`; tham chiếu @fig:x @tab:x @eq:x @sec:x
//   - mục liệt kê (thụt 2 dấu cách = cấp 2), 1. mục đánh số
//   Table: chú thích {#tab:id w=20,30,50}   rồi bảng | a | b | (dòng 2 là |---|)
//   ![chú thích](hinh/tep.png){#fig:id w=15}   (w tính bằng cm)
//   $$ten$$ {#eq:id}                           công thức hinh/eq_ten.png
//   ```  khối mã  ```
//   > [!TODO] nội dung                         ô ghi chú vàng: phần chờ bổ sung
//   > nội dung                                 ô ghi chú xám
//   \pagebreak
const fs = require("fs");
const path = require("path");
const d = require("docx");
const {
  Document, Packer, Paragraph, TextRun, ImageRun, Table, TableRow, TableCell, WidthType, AlignmentType,
  HeadingLevel, TableOfContents, StyleLevel, PageBreak, Header, Footer, PageNumber, NumberFormat,
  ShadingType, BorderStyle, LevelFormat, TabStopType, VerticalAlign, TableLayoutType,
} = d;

const GOC = __dirname;
const CHU = "Times New Roman";
const MONO = "Courier New";
const CM = 567;                      // DXA mỗi cm
const RONG_TRANG = 21 * CM, LE_TRAI = 3 * CM, LE_PHAI = 2 * CM;
const RONG_CHU = RONG_TRANG - LE_TRAI - LE_PHAI;   // 9072 DXA = 16 cm
const PX_CM = 37.795;                // docx-js tính ảnh theo px ở 96 dpi

// ------------------------------------------------------------------ ngôn ngữ
const CHINH = process.argv.includes("chinh");
const EN = process.argv.includes("en");          // node tao_bao_cao.js chinh en -> báo cáo chính tiếng Anh
const VI = !EN && (CHINH || process.argv.includes("vi"));
const L = VI ? {
  thuMuc: "noi_dung_vi", ra: "BD03_Bao_cao_He_thong_goi_y.docx",
  chuong: "Chương ", muc: "Mục ", hinh: "Hình ", bang: "Bảng ", ct: "Công thức ", phuLuc: "Phụ lục ",
  mucLuc: "Mục lục", dsHinh: "Danh mục hình", dsBang: "Danh mục bảng",
  dauTrang: "Hệ thống gợi ý sản phẩm thương mại điện tử – Báo cáo đồ án",
  truong: "TRƯỜNG ĐẠI HỌC TÂN TẠO", khoa: "KHOA CÔNG NGHỆ THÔNG TIN", loai: "BÁO CÁO ĐỒ ÁN CUỐI KỲ",
  tenDeTai: "HỆ THỐNG GỢI Ý SẢN PHẨM THƯƠNG MẠI ĐIỆN TỬ", monHoc: "Học phần: Dữ liệu lớn (CS411V)", hocKy: "Học kỳ Thu 2026",
  nhom: "THÀNH VIÊN NHÓM", tv: "Thành viên", ma: "MSSV", gv: "Giảng viên hướng dẫn: TS. Cao Tiến Dũng",
  ten1: "Lê Chí Tâm", ten2: "Lê Nguyễn Quốc Anh",
  tieuDeTep: "Hệ thống gợi ý sản phẩm thương mại điện tử – Báo cáo đồ án (CS411V Dữ liệu lớn)",
} : {
  thuMuc: "noi_dung", ra: "BD03_Recommender_System_Report.docx",
  chuong: "Chapter ", muc: "Section ", hinh: "Figure ", bang: "Table ", ct: "Equation ", phuLuc: "Appendix ",
  mucLuc: "Table of Contents", dsHinh: "List of Figures", dsBang: "List of Tables",
  dauTrang: "E-commerce Product Recommender System – Final Report",
  truong: "TAN TAO UNIVERSITY", khoa: "SCHOOL OF INFORMATION TECHNOLOGY", loai: "Report of Final Project",
  tenDeTai: "E-COMMERCE PRODUCT RECOMMENDER SYSTEM", monHoc: "Course: Big Data (CS411V)", hocKy: "Fall 2026",
  nhom: "GROUP MEMBER", tv: "Member", ma: "Student code", gv: "Supervisor: Cao Tien Dung, PhD",
  ten1: "Le Chi Tam", ten2: "Le Nguyen Quoc Anh",
  tieuDeTep: "E-commerce Product Recommender System – Final Project Report (CS411 Big Data)",
};

// ------------------------------------------------------------------ đọc nội dung
if (CHINH && VI) Object.assign(L, {
  thuMuc: "noi_dung_chinh", ra: "BD03_Bao_cao_chinh.docx", loai: "BÁO CÁO ĐỒ ÁN CUỐI KỲ",
  dauTrang: "Hệ thống gợi ý sản phẩm thương mại điện tử – Báo cáo chính",
  tieuDeTep: "Hệ thống gợi ý sản phẩm thương mại điện tử – Báo cáo chính (CS411V Dữ liệu lớn)",
});
if (CHINH && EN) Object.assign(L, {
  thuMuc: "noi_dung_chinh_en", ra: "BD03_Main_Report_EN.docx", loai: "FINAL PROJECT REPORT",
  nhom: "GROUP MEMBERS", ma: "Student ID",
  dauTrang: "E-commerce Product Recommender System – Main Report",
  tieuDeTep: "E-commerce Product Recommender System – Main Report (CS411V Big Data)",
});
const tep = fs.readdirSync(path.join(GOC, L.thuMuc)).filter((t) => /^\d\d_.*\.md$/.test(t)).sort();
const nguon = tep.map((t) => ({ ten: t, dong: fs.readFileSync(path.join(GOC, L.thuMuc, t), "utf8").split(/\r?\n/) }));

// ------------------------------------------------------------------ lượt 1: đánh số và nhãn
const nhan = {};
let chuong = 0, phuLuc = null, m2 = 0, m3 = 0, soHinh = 0, soBang = 0, soCT = 0;
const tienTo = () => (phuLuc ? phuLuc : String(chuong));
for (const f of nguon) {
  for (const dong of f.dong) {
    let m;
    if ((m = dong.match(/^(#{1,3}) (.*)$/))) {
      const cap = m[1].length;
      let tieuDe = m[2];
      const id = (tieuDe.match(/\{#(sec:[\w-]+)\}/) || [])[1];
      tieuDe = tieuDe.replace(/\s*\{#[^}]+\}/, "");
      if (cap === 1) {
        m2 = m3 = soHinh = soBang = soCT = 0;
        if (tieuDe.startsWith("@")) { phuLuc = tieuDe.slice(1, 2); }
        else if (tieuDe.startsWith("!")) { /* không đánh số */ }
        else { phuLuc = null; chuong += 1; }
        if (id) nhan[id] = phuLuc ? L.phuLuc + phuLuc : L.chuong + chuong;
      } else if (cap === 2) { m2 += 1; m3 = 0; if (id) nhan[id] = L.muc + tienTo() + "." + m2; }
      else { m3 += 1; if (id) nhan[id] = L.muc + tienTo() + "." + m2 + "." + m3; }
    } else if ((m = dong.match(/^!\[.*\]\(.*\)\{([^}]*)\}/))) {
      soHinh += 1; const id = (m[1].match(/#(fig:[\w-]+)/) || [])[1];
      if (id) nhan[id] = L.hinh + tienTo() + "." + soHinh;
    } else if ((m = dong.match(/^Table: (?!- )(?!-$).*\{([^}]*)\}/))) {
      soBang += 1; const id = (m[1].match(/#(tab:[\w-]+)/) || [])[1];
      if (id) nhan[id] = L.bang + tienTo() + "." + soBang;
    } else if ((m = dong.match(/^\$\$(\w+)\$\$\s*\{#(eq:[\w-]+)\}/))) {
      soCT += 1; nhan[m[2]] = L.ct + "(" + tienTo() + "." + soCT + ")";
    }
  }
}

// ------------------------------------------------------------------ chữ trong đoạn
function thayThamChieu(s) {
  return s.replace(/@((?:fig|tab|eq|sec):[\w-]+)/g, (_, id) => {
    if (!nhan[id]) { console.warn("!! thiếu nhãn", id); return "??"; }
    return nhan[id];
  });
}
function chiSoTrenDuoi(s, co, them, coChiSoDuoi) {
  // ^{...} -> chỉ số trên ở mọi nơi; x_y -> chỉ số dưới chỉ trong chữ nghiêng (biến toán học)
  const ra = [];
  const re = coChiSoDuoi ? /\^\{([^}]*)\}|([A-Za-zλαβ])_([A-Za-z0-9]+)/g : /\^\{([^}]*)\}/g;
  let i = 0, m;
  while ((m = re.exec(s))) {
    if (m.index > i) ra.push(new TextRun({ text: s.slice(i, m.index), font: CHU, size: co, ...them }));
    if (m[1] !== undefined) ra.push(new TextRun({ text: m[1], font: CHU, size: co, superScript: true, ...them }));
    else {
      ra.push(new TextRun({ text: m[2], font: CHU, size: co, ...them }));
      ra.push(new TextRun({ text: m[3], font: CHU, size: co, subScript: true, ...them }));
    }
    i = m.index + m[0].length;
  }
  if (i < s.length) ra.push(new TextRun({ text: s.slice(i), font: CHU, size: co, ...them }));
  return ra;
}
function phanTich(s, co, them) {
  const ra = [];
  const re = /(\*\*[^*]+\*\*|`[^`]+`|\*[^*\s][^*]*\*)/g;
  let i = 0, m;
  while ((m = re.exec(s))) {
    if (m.index > i) ra.push(...chiSoTrenDuoi(s.slice(i, m.index), co, them, false));
    const t = m[0];
    if (t.startsWith("**")) ra.push(...phanTich(t.slice(2, -2), co, { ...them, bold: true }));
    else if (t.startsWith("`")) ra.push(new TextRun({ text: t.slice(1, -1), font: MONO, size: Math.round(co * 0.85), ...them }));
    else ra.push(...chiSoTrenDuoi(t.slice(1, -1), co, { ...them, italics: true }, true));
    i = m.index + t.length;
  }
  if (i < s.length) ra.push(...chiSoTrenDuoi(s.slice(i), co, them, false));
  return ra;
}
function chuTrongDoan(s, co, them = {}) {
  return phanTich(thayThamChieu(s), co, them);
}

// ------------------------------------------------------------------ ảnh
function kichThuocPng(tep) {
  const b = fs.readFileSync(tep);
  return { rong: b.readUInt32BE(16), cao: b.readUInt32BE(20), du_lieu: b };
}
function anh(tepAnh, rongCm, caoToiDaCm = 23) {
  const k = kichThuocPng(path.join(GOC, tepAnh));
  let w = rongCm * PX_CM, h = (w * k.cao) / k.rong;
  if (h > caoToiDaCm * PX_CM) { h = caoToiDaCm * PX_CM; w = (h * k.rong) / k.cao; }
  return new ImageRun({ type: "png", data: k.du_lieu, transformation: { width: Math.round(w), height: Math.round(h) } });
}

function anhCongThuc(tepAnh) {
  // ảnh công thức vẽ ở 220 dpi, cỡ chữ 15 pt; hiển thị tương đương 13 pt
  const k = kichThuocPng(path.join(GOC, tepAnh));
  let w = (k.rong * 96) / 220 * 0.87, h = (k.cao * 96) / 220 * 0.87;
  const toiDa = 13.2 * PX_CM;
  if (w > toiDa) { h = (h * toiDa) / w; w = toiDa; }
  return new ImageRun({ type: "png", data: k.du_lieu, transformation: { width: Math.round(w), height: Math.round(h) } });
}

// ------------------------------------------------------------------ bảng
const VIEN = { style: BorderStyle.SINGLE, size: 4, color: "808080" };
function bang(dongBang, tiLe) {
  const hang = dongBang.filter((l) => !/^\|\s*:?-{2,}/.test(l)).map((l) =>
    l.trim().replace(/^\|/, "").replace(/\|$/, "").split(/(?<!\\)\|/).map((c) => c.trim().replace(/\\\|/g, "|")));
  const soCot = hang[0].length;
  let ti = tiLe && tiLe.length === soCot ? tiLe : null;
  if (!ti) {   // tự chia theo độ dài chữ, có sàn và trần
    // tiêu đề in đậm nên cần thêm chỗ; tính theo từ dài nhất để không gãy giữa từ (vd. NDCG@10)
    const dai = Array.from({ length: soCot }, (_, j) => Math.max(...hang.map((r, i) => i === 0
      ? Math.max(...(r[j] || "").split(/\s+/).map((t) => t.length + 3), Math.ceil((r[j] || "").length / 2))
      : (r[j] || "").length)));
    const tong = dai.reduce((a, b) => a + Math.min(Math.max(b, 4), 60), 0);
    ti = dai.map((x) => (100 * Math.min(Math.max(x, 4), 60)) / tong);
  }
  const tongTi = ti.reduce((a, b) => a + b, 0);
  const rong = ti.map((x) => Math.floor((RONG_CHU * x) / tongTi));
  rong[rong.length - 1] = RONG_CHU - rong.slice(0, -1).reduce((a, b) => a + b, 0);
  const so = (s) => /^[-+−]?[\d.,%]+( ?%| ms| s| GB)?$/.test(s.replace(/\*\*/g, ""));
  const coChu = soCot >= 8 ? 17 : soCot >= 6 ? 19 : 20;
  return new Table({
    width: { size: RONG_CHU, type: WidthType.DXA }, columnWidths: rong, layout: TableLayoutType.FIXED,
    alignment: AlignmentType.CENTER,
    borders: { top: VIEN, bottom: VIEN, left: VIEN, right: VIEN, insideHorizontal: VIEN, insideVertical: VIEN },
    rows: hang.map((r, i) => new TableRow({ // bảng ≤ 14 dòng: giữ liền một trang; bảng dài: giữ tiêu đề với 4 dòng đầu
      tableHeader: i === 0, cantSplit: true,
      children: r.map((c, j) => new TableCell({
        width: { size: rong[j], type: WidthType.DXA },
        shading: i === 0 ? { type: ShadingType.CLEAR, fill: "DCE6F2", color: "auto" } : undefined,
        margins: { top: 40, bottom: 40, left: 80, right: 80 }, verticalAlign: VerticalAlign.CENTER,
        children: [new Paragraph({
          keepNext: (hang.length <= 14 && i < hang.length - 1) || (hang.length > 14 && i < 4),
          spacing: { before: 0, after: 0, line: 240, lineRule: "auto" },
          alignment: i > 0 && so(c) ? AlignmentType.RIGHT : AlignmentType.LEFT,
          children: chuTrongDoan(c, coChu, i === 0 ? { bold: true } : {}),
        })],
      })),
    })),
  });
}

// ------------------------------------------------------------------ lượt 2: dựng nội dung
let dsLietKe = 0;
function dung(dongs) {
  const ra = [];
  chuong = 0; phuLuc = null; m2 = m3 = soHinh = soBang = soCT = 0;
  let i = 0, tieuDeDau = true;
  const doanCho = [];
  const xaDoan = () => {
    if (!doanCho.length) return;
    ra.push(new Paragraph({ style: "Than", children: chuTrongDoan(doanCho.join(" "), 26) }));
    doanCho.length = 0;
  };
  while (i < dongs.length) {
    const dong = dongs[i];
    let m;
    if (/^\s*$/.test(dong)) { xaDoan(); i++; continue; }
    if (dong.trim() === "\\pagebreak") { xaDoan(); ra.push(new Paragraph({ children: [new PageBreak()] })); i++; continue; }
    if ((m = dong.match(/^(#{1,4}) (.*)$/))) {
      xaDoan();
      const cap = m[1].length;
      let t = m[2].replace(/\s*\{#[^}]+\}/, "");
      let so = "";
      if (cap === 1) {
        m2 = m3 = soHinh = soBang = soCT = 0;
        if (t.startsWith("@")) { phuLuc = t.slice(1, 2); so = L.phuLuc + phuLuc + ". "; t = t.slice(3); }
        else if (t.startsWith("!")) { t = t.slice(1); }
        else { phuLuc = null; chuong += 1; so = chuong + "  "; }
      } else if (cap === 2) { m2 += 1; m3 = 0; so = tienTo() + "." + m2 + "  "; }
      else if (cap === 3) { m3 += 1; so = tienTo() + "." + m2 + "." + m3 + "  "; }
      const muc = [HeadingLevel.HEADING_1, HeadingLevel.HEADING_2, HeadingLevel.HEADING_3, HeadingLevel.HEADING_4][cap - 1];
      // báo cáo chính (giới hạn trang): chương nối tiếp, chỉ ngắt trang trước chương 1 và phụ lục A
      const ngatTrang = cap === 1 && !tieuDeDau && (!CHINH || (phuLuc ? phuLuc === "A" : chuong === 1));
      ra.push(new Paragraph({ heading: muc, pageBreakBefore: ngatTrang, children: [new TextRun({ text: so + t })] }));
      tieuDeDau = false;
      i++; continue;
    }
    if (dong.startsWith("```")) {
      xaDoan(); i++;
      const ma = [];
      while (i < dongs.length && !dongs[i].startsWith("```")) { ma.push(dongs[i]); i++; }
      i++;
      ma.forEach((l, k) => ra.push(new Paragraph({
        style: "Ma", keepNext: k < ma.length - 1,
        spacing: { before: k === 0 ? 80 : 0, after: k === ma.length - 1 ? 160 : 0, line: 240, lineRule: "auto" },
        children: [new TextRun({ text: l.length ? l : " ", font: MONO, size: 15 })],
      })));
      continue;
    }
    if ((m = dong.match(/^> \[!TODO\] (.*)$/))) {
      xaDoan();
      ra.push(new Paragraph({ style: "GhiChu", shading: { type: ShadingType.CLEAR, fill: "FFF2CC", color: "auto" },
        border: { left: { style: BorderStyle.SINGLE, size: 18, color: "BF9000", space: 6 } },
        children: [new TextRun({ text: "To be completed: ", bold: true, font: CHU, size: 24, color: "7F6000" }), ...chuTrongDoan(m[1], 24)] }));
      i++; continue;
    }
    if ((m = dong.match(/^> (.*)$/))) {
      xaDoan();
      ra.push(new Paragraph({ style: "GhiChu", shading: { type: ShadingType.CLEAR, fill: "F2F2F2", color: "auto" },
        border: { left: { style: BorderStyle.SINGLE, size: 18, color: "7F7F7F", space: 6 } },
        children: chuTrongDoan(m[1], 24) }));
      i++; continue;
    }
    if ((m = dong.match(/^!\[(.*)\]\((.*)\)\{([^}]*)\}/))) {
      xaDoan(); soHinh += 1;
      const w = parseFloat((m[3].match(/w=([\d.]+)/) || [])[1] || "15.5");
      const cao = parseFloat((m[3].match(/h=([\d.]+)/) || [])[1] || "22");
      ra.push(new Paragraph({ alignment: AlignmentType.CENTER, keepNext: true, spacing: { before: 120, after: 60, line: 240, lineRule: "auto" }, children: [anh(m[2], w, cao)] }));
      ra.push(new Paragraph({ style: "ChuThichHinh", children: [new TextRun({ text: L.hinh + tienTo() + "." + soHinh + ". ", bold: true }), ...chuTrongDoan(m[1], 24, { italics: true })] }));
      i++; continue;
    }
    if ((m = dong.match(/^\$\$(\w+)\$\$/))) {
      xaDoan(); soCT += 1;
      const tepCT = "hinh/eq_" + m[1] + ".png";
      const k = kichThuocPng(path.join(GOC, tepCT));
      let wPx = (k.rong * 96) / 220 * 0.87;
      const toiDaPx = 13.2 * PX_CM;
      if (wPx > toiDaPx) wPx = toiDaPx;
      const wDxa = (wPx / PX_CM) * CM;
      let thut = Math.max(0, (RONG_CHU - wDxa) / 2);
      thut = Math.min(thut, RONG_CHU - wDxa - 1.5 * CM);
      ra.push(new Paragraph({
        tabStops: [{ type: TabStopType.RIGHT, position: RONG_CHU }],
        indent: { left: Math.round(Math.max(0, thut)) },
        spacing: { before: 120, after: 120, line: 240, lineRule: "auto" }, keepLines: true,
        children: [anhCongThuc(tepCT), new TextRun({ text: "\t(" + tienTo() + "." + soCT + ")", font: CHU, size: 26 })],
      }));
      i++; continue;
    }
    if ((m = dong.match(/^Table: (.*?)\s*(\{([^}]*)\})?$/))) {
      xaDoan();
      const thuocTinh = m[3] || "";
      const ti = (thuocTinh.match(/w=([\d.,]+)/) || [])[1];
      if (m[1] !== "-") {
        soBang += 1;
        ra.push(new Paragraph({ style: "ChuThichBang", keepNext: true, children: [new TextRun({ text: L.bang + tienTo() + "." + soBang + ". ", bold: true }), ...chuTrongDoan(m[1], 24, { italics: true })] }));
      }
      i++;
      const dongBang = [];
      while (i < dongs.length && dongs[i].trim().startsWith("|")) { dongBang.push(dongs[i]); i++; }
      ra.push(bang(dongBang, ti ? ti.split(",").map(Number) : null));
      ra.push(new Paragraph({ spacing: { before: 0, after: 120, line: 240, lineRule: "auto" }, children: [] }));
      continue;
    }
    if ((m = dong.match(/^(\s*)([-*]|\d+\.) (.*)$/))) {
      xaDoan();
      const laSo = /\d/.test(m[2]);
      if (laSo && (i === 0 || !/^\s*\d+\. /.test(dongs[i - 1]))) dsLietKe += 1;
      let noiDung = m[3];
      const cap = Math.min(Math.floor(m[1].length / 2), 1);
      i++;
      while (i < dongs.length && /^\s{2,}\S/.test(dongs[i]) && !/^\s*([-*]|\d+\.) /.test(dongs[i])) { noiDung += " " + dongs[i].trim(); i++; }
      ra.push(new Paragraph({
        style: "Than", spacing: { before: 0, after: 60, line: 360, lineRule: "auto" },
        numbering: laSo ? { reference: "so", level: cap, instance: dsLietKe } : { reference: "cham", level: cap },
        children: chuTrongDoan(noiDung, 26),
      }));
      continue;
    }
    doanCho.push(dong.trim()); i++;
  }
  xaDoan();
  return ra;
}

// ------------------------------------------------------------------ bìa và phần đầu
function dongGiua(text, co, them = {}, sau = 120) {
  return new Paragraph({ alignment: AlignmentType.CENTER, spacing: { before: 0, after: sau, line: 276, lineRule: "auto" },
    children: [new TextRun({ text, font: CHU, size: co, ...them })] });
}
// Bìa theo mẫu bìa đồ án của Trường (khung viền đôi dày–mảnh, logo, thông tin nhóm, giảng viên, GitHub).
// Khung là viền trang của riêng phần bìa, cách vùng chữ 12 pt; lề phần bìa được nới để khung nằm
// ở vị trí như mẫu (trái 3,2 cm, phải 1,7 cm, trên 3,2 cm, dưới 2,2 cm tính từ mép giấy).
const BIA_LE = { top: 3.62 * CM, bottom: 2.62 * CM, left: 3.62 * CM, right: 2.12 * CM };
// thinThickSmallGap = nét mảnh nằm trong nét dày (ECMA-376 17.18.2): nét dày ở ngoài, nét mảnh ở trong như mẫu
const VIEN_BIA = { style: BorderStyle.THIN_THICK_SMALL_GAP, size: 24, color: "000000", space: 12 };
const REPO = "https://github.com/chitam31/CS411V_Final-Project_Ecommerce-Recommender-System";
function dongBia(chuoi, co, them = {}, sau = 200, truoc = 0) {
  return new Paragraph({ alignment: AlignmentType.CENTER, spacing: { before: truoc, after: sau, line: 276, lineRule: "auto" },
    children: [new TextRun({ text: chuoi, font: CHU, size: co, bold: true, ...them })] });
}
const anhLogo = fs.readFileSync(path.join(GOC, "hinh", "logo_ttu.png"));
const RONG_LOGO = 6.0 * PX_CM;
function dongThanhVien(stt, ten, ma, sau) {
  return new Paragraph({ spacing: { before: 0, after: sau, line: 276, lineRule: "auto" }, indent: { left: 0.58 * CM },
    tabStops: [{ type: TabStopType.LEFT, position: 9.48 * CM }],
    children: [new TextRun({ text: `${L.tv} ${stt}: ${ten}`, font: CHU, size: 26, bold: true }),
      new TextRun({ text: `\t${L.ma}: ${ma}`, font: CHU, size: 26, bold: true })] });
}
const bia = [
  dongBia(L.truong, 28, {}, 240, 480),
  dongBia(L.khoa, 28, {}, 280),
  new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 120 },
    children: [new ImageRun({ type: "png", data: anhLogo, transformation: { width: RONG_LOGO, height: RONG_LOGO * 588 / 798 } })] }),
  dongBia(L.loai, 26, {}, 220),
  dongBia(L.tenDeTai, 32, {}, 220),
  dongBia(L.monHoc, 26, {}, 220),
  dongBia(L.hocKy, 26, { italics: true }, 900),
  dongBia(L.nhom, 26, {}, 240),
  dongThanhVien(1, L.ten1, "2302220", 240),
  dongThanhVien(2, L.ten2, "2302283", 2400),
  dongBia(L.gv, 26, {}, 200),
  new Paragraph({ alignment: AlignmentType.CENTER, spacing: { after: 0, line: 276, lineRule: "auto" }, children: [
    new TextRun({ text: "Github: ", font: CHU, size: 22, bold: true, italics: true }),
    new d.ExternalHyperlink({ link: REPO, children: [new TextRun({ text: REPO, font: CHU, size: 21, bold: true, italics: true, color: "0000EE", underline: {} })] }),
  ] }),
];

const tieuDeKhongSo = (t) => new Paragraph({ style: "TieuDeMucLuc", pageBreakBefore: true, children: [new TextRun({ text: t })] });
const phanDau = [];
// 00_a*: trước mục lục (tóm tắt); 00_b*: sau danh mục hình/bảng (viết tắt, quy ước tên)
const truocMucLuc = [].concat(...nguon.filter((f) => f.ten.startsWith("00_a")).map((f) => f.dong.concat([""])));
const sauMucLuc = [].concat(...nguon.filter((f) => f.ten.startsWith("00_b")).map((f) => f.dong.concat([""])));
const chinh = nguon.filter((f) => !f.ten.startsWith("00_"));
phanDau.push(...dung(truocMucLuc));
phanDau.push(tieuDeKhongSo(L.mucLuc));
phanDau.push(new TableOfContents(L.mucLuc, { hyperlink: true, headingStyleRange: CHINH ? "1-2" : "1-3" }));
// Bản chính (15–20 trang) bỏ danh mục hình/bảng để dành trang cho nội dung.
if (!CHINH) {
  phanDau.push(tieuDeKhongSo(L.dsHinh));
  phanDau.push(new TableOfContents(L.dsHinh, { hyperlink: true, stylesWithLevels: [new StyleLevel("Figure Caption", 1)] }));
  phanDau.push(tieuDeKhongSo(L.dsBang));
  phanDau.push(new TableOfContents(L.dsBang, { hyperlink: true, stylesWithLevels: [new StyleLevel("Table Caption", 1)] }));
}
const sau = dung(sauMucLuc);
if (sau.length) phanDau.push(new Paragraph({ children: [new PageBreak()] }), ...sau);

const than = dung([].concat(...chinh.map((f) => f.dong.concat([""]))));
// chương đầu tiên không cần ngắt trang vì đã sang phần mới
// (pageBreakBefore của tiêu đề đầu tiên tạo trang trắng): bỏ cho đoạn đầu
// docx-js không cho sửa sau khi tạo, nên dựng lại phần tử đầu
const hdr = (t) => new Header({ children: [new Paragraph({ alignment: AlignmentType.RIGHT, border: { bottom: { style: BorderStyle.SINGLE, size: 4, color: "A6A6A6", space: 2 } },
  children: [new TextRun({ text: t, font: CHU, size: 19, italics: true, color: "595959" })] })] });
const ftr = () => new Footer({ children: [new Paragraph({ alignment: AlignmentType.CENTER, children: [new TextRun({ children: [PageNumber.CURRENT], font: CHU, size: 22 })] })] });
const trang = (batDau, kieu) => ({ size: { width: RONG_TRANG, height: 29.7 * CM }, margin: { top: 2 * CM, bottom: 2 * CM, left: LE_TRAI, right: LE_PHAI, header: 0.9 * CM, footer: 0.9 * CM },
  pageNumbers: { start: batDau, formatType: kieu } });

const doc = new Document({
  creator: "Le Chi Tam, Le Nguyen Quoc Anh",
  title: L.tieuDeTep,
  description: "Topic 3, bd03_khuyen_nghi",
  features: { updateFields: true },
  styles: {
    default: { document: { run: { font: CHU, size: 26 }, paragraph: { spacing: { line: 360, lineRule: "auto" } } } },
    paragraphStyles: [
      { id: "Heading1", name: "Heading 1", basedOn: "Normal", next: "Than", quickFormat: true,
        run: { font: CHU, size: 32, bold: true, color: "1F3864" }, paragraph: { spacing: { before: 0, after: 300, line: 276, lineRule: "auto" }, keepNext: true, outlineLevel: 0 } },
      { id: "Heading2", name: "Heading 2", basedOn: "Normal", next: "Than", quickFormat: true,
        run: { font: CHU, size: 28, bold: true, color: "1F3864" }, paragraph: { spacing: { before: 280, after: 120, line: 276, lineRule: "auto" }, keepNext: true, outlineLevel: 1 } },
      { id: "Heading3", name: "Heading 3", basedOn: "Normal", next: "Than", quickFormat: true,
        run: { font: CHU, size: 26, bold: true }, paragraph: { spacing: { before: 200, after: 80, line: 276, lineRule: "auto" }, keepNext: true, outlineLevel: 2 } },
      { id: "Heading4", name: "Heading 4", basedOn: "Normal", next: "Than", quickFormat: true,
        run: { font: CHU, size: 26, bold: true, italics: true }, paragraph: { spacing: { before: 160, after: 60, line: 276, lineRule: "auto" }, keepNext: true } },
      { id: "TieuDeMucLuc", name: "Contents Heading Report", basedOn: "Normal", next: "Normal",
        run: { font: CHU, size: 32, bold: true, color: "1F3864" }, paragraph: { spacing: { before: 0, after: 300, line: 276, lineRule: "auto" } } },
      { id: "Than", name: "Body Text Report", basedOn: "Normal", run: { font: CHU, size: 26 },
        paragraph: { alignment: AlignmentType.JUSTIFIED, spacing: { before: 0, after: 120, line: 360, lineRule: "auto" } } },
      { id: "ChuThichHinh", name: "Figure Caption", basedOn: "Normal", run: { font: CHU, size: 24 },
        paragraph: { alignment: AlignmentType.CENTER, spacing: { before: 40, after: 240, line: 276, lineRule: "auto" } } },
      { id: "ChuThichBang", name: "Table Caption", basedOn: "Normal", run: { font: CHU, size: 24 },
        paragraph: { alignment: AlignmentType.CENTER, spacing: { before: 200, after: 80, line: 276, lineRule: "auto" }, keepNext: true } },
      { id: "Ma", name: "Code Block", basedOn: "Normal", run: { font: MONO, size: 17 },
        paragraph: { shading: { type: ShadingType.CLEAR, fill: "F3F3F3", color: "auto" }, indent: { left: 85, right: 85 } } },
      { id: "GhiChu", name: "Note Box", basedOn: "Normal", run: { font: CHU, size: 24 },
        paragraph: { spacing: { before: 120, after: 160, line: 300, lineRule: "auto" }, indent: { left: 200, right: 200 } } },
    ],
  },
  numbering: { config: [
    { reference: "cham", levels: [
      { level: 0, format: LevelFormat.BULLET, text: "•", alignment: AlignmentType.LEFT, style: { paragraph: { indent: { left: 567, hanging: 284 } } } },
      { level: 1, format: LevelFormat.BULLET, text: "–", alignment: AlignmentType.LEFT, style: { paragraph: { indent: { left: 1134, hanging: 284 } } } }] },
    { reference: "so", levels: [
      { level: 0, format: LevelFormat.DECIMAL, text: "%1.", alignment: AlignmentType.LEFT, style: { paragraph: { indent: { left: 567, hanging: 340 } } } },
      { level: 1, format: LevelFormat.LOWER_LETTER, text: "%2)", alignment: AlignmentType.LEFT, style: { paragraph: { indent: { left: 1134, hanging: 340 } } } }] },
  ] },
  sections: [
    { properties: { page: { size: { width: RONG_TRANG, height: 29.7 * CM }, margin: { ...BIA_LE, header: 0.9 * CM, footer: 0.9 * CM },
        borders: { pageBorders: { display: d.PageBorderDisplay.ALL_PAGES, offsetFrom: d.PageBorderOffsetFrom.TEXT },
          pageBorderTop: VIEN_BIA, pageBorderLeft: VIEN_BIA, pageBorderBottom: VIEN_BIA, pageBorderRight: VIEN_BIA } } }, children: bia },
    { properties: { page: trang(1, NumberFormat.LOWER_ROMAN) }, headers: { default: hdr(L.dauTrang) }, footers: { default: ftr() }, children: phanDau },
    { properties: { page: trang(1, NumberFormat.DECIMAL) }, headers: { default: hdr(L.dauTrang) }, footers: { default: ftr() }, children: than },
  ],
});

const ra = path.join(GOC, L.ra);
Packer.toBuffer(doc).then((b) => { fs.writeFileSync(ra, b); console.log("đã ghi", ra, Math.round(b.length / 1024), "KB; nhãn:", Object.keys(nhan).length); });
