use chrono::Datelike;
use printpdf::*;

use crate::types::{RestraintDayRow, RestraintReportResponse, WeeklySubtotal};

const FONT_DATA: &[u8] = include_bytes!("../assets/fonts/NotoSansJP-Regular.ttf");

// A4 portrait
const PAGE_W: f32 = 210.0;
const PAGE_H: f32 = 297.0;
const MARGIN_LEFT: f32 = 5.0;
const MARGIN_RIGHT: f32 = 5.0;
const MARGIN_TOP: f32 = 8.0;

// Column widths (mm) — 元PDF準拠レイアウト
// 日付 | 始業 | 終業 | 運転 | 荷役 | 休憩 | 小計 | 合計 | 累計 | 運転平均 | 休息 | 実働 | 時間外 | 深夜 | 時間外深夜 | 摘要
const COL_DATE: f32 = 9.0;
const COL_START: f32 = 11.0;
const COL_END: f32 = 11.0;
const COL_DRIVE: f32 = 11.0;
const COL_CARGO: f32 = 11.0;
const COL_BREAK: f32 = 11.0;
const COL_SUBTOTAL: f32 = 12.0;
const COL_TOTAL: f32 = 12.0;
const COL_CUMULATIVE: f32 = 13.0;
const COL_DRIVE_AVG: f32 = 11.0;
const COL_REST: f32 = 12.0;
const COL_ACTUAL: f32 = 12.0;
const COL_OVERTIME: f32 = 11.0;
const COL_NIGHT: f32 = 11.0;
const COL_OT_NIGHT: f32 = 11.0;

const ROW_H: f32 = 7.0; // 日付行の高さ（重複下段用に高め）
const ROW_H_SINGLE: f32 = 5.0; // 重複なし日の行高
const HEADER_ROW_H: f32 = 5.5;
const SUBTOTAL_ROW_H: f32 = 5.0;
const FONT_SIZE_HEADER: f32 = 9.0;
const FONT_SIZE_BODY: f32 = 6.5;
const FONT_SIZE_SMALL: f32 = 5.5;
const FONT_SIZE_OVERLAP: f32 = 5.5; // 重複下段
const FONT_SIZE_TITLE: f32 = 12.0;
const LINE_THIN: f32 = 0.2;
const LINE_THICK: f32 = 0.5;

fn col_widths() -> Vec<f32> {
    vec![
        COL_DATE,
        COL_START,
        COL_END,
        COL_DRIVE,
        COL_CARGO,
        COL_BREAK,
        COL_SUBTOTAL,
        COL_TOTAL,
        COL_CUMULATIVE,
        COL_DRIVE_AVG,
        COL_REST,
        COL_ACTUAL,
        COL_OVERTIME,
        COL_NIGHT,
        COL_OT_NIGHT,
    ]
}

fn table_width() -> f32 {
    col_widths().iter().sum::<f32>()
}

fn col_remarks() -> f32 {
    let remaining = PAGE_W - MARGIN_LEFT - MARGIN_RIGHT - table_width();
    remaining.max(2.0)
}

fn fmt_minutes(val: i32) -> String {
    if val == 0 {
        return String::new();
    }
    let h = val / 60;
    let m = val % 60;
    format!("{}:{:02}", h, m)
}

/// 日付行に重複があるかどうかで行高を決定
fn day_row_height(day: &RestraintDayRow) -> f32 {
    if day.overlap_restraint_minutes > 0 {
        ROW_H
    } else {
        ROW_H_SINGLE
    }
}

// ===== PDF Generation =====

pub fn generate_pdf(
    reports: &[RestraintReportResponse],
    driver_cds: &[String],
    year: i32,
    month: u32,
) -> Vec<u8> {
    let mut doc = PdfDocument::new("拘束時間管理表");
    let mut warnings = Vec::new();

    let font =
        ParsedFont::from_bytes(FONT_DATA, 0, &mut warnings).expect("embedded font must parse");
    let font_id = doc.add_font(&font);

    let mut pages = Vec::new();
    for (i, report) in reports.iter().enumerate() {
        let driver_cd = driver_cds.get(i).map(|s| s.as_str()).unwrap_or("");
        let page = render_driver_page(&doc, &font_id, report, driver_cd, year, month);
        pages.push(page);
    }

    doc.with_pages(pages);

    let opts = PdfSaveOptions::default();
    doc.save(&opts, &mut warnings)
}

fn render_driver_page(
    doc: &PdfDocument,
    font_id: &FontId,
    report: &RestraintReportResponse,
    driver_cd: &str,
    year: i32,
    month: u32,
) -> PdfPage {
    let mut ops = Vec::new();
    let widths = col_widths();
    let remarks_w = col_remarks();
    let total_w: f32 = widths.iter().sum::<f32>() + remarks_w;
    let all_widths: Vec<f32> = {
        let mut w = widths.clone();
        w.push(remarks_w);
        w
    };

    // --- Title / header info ---
    let reiwa_year = year - 2018;
    let mut y = PAGE_H - MARGIN_TOP;
    let table_x = MARGIN_LEFT;

    add_text(
        &mut ops,
        doc,
        font_id,
        MARGIN_LEFT,
        y,
        FONT_SIZE_TITLE,
        "拘 束 時 間 管 理 表",
    );
    add_text(
        &mut ops,
        doc,
        font_id,
        MARGIN_LEFT + 68.0,
        y,
        FONT_SIZE_HEADER,
        &format!("{}年 {}月分", year, month),
    );
    y -= 5.0;

    let name_label = format!("氏 名　( {} ) {}", driver_cd, report.driver_name);
    add_text(
        &mut ops,
        doc,
        font_id,
        MARGIN_LEFT,
        y,
        FONT_SIZE_HEADER,
        &name_label,
    );
    y -= 4.0;

    let max_label = format!(
        "当月の最大拘束時間: {}時間",
        report.max_restraint_minutes / 60
    );
    add_text_right(
        &mut ops,
        doc,
        font_id,
        PAGE_W - MARGIN_RIGHT,
        y,
        FONT_SIZE_SMALL,
        &max_label,
    );
    y -= 4.0;

    // 運転平均列の位置（ヘッダーでも使用）
    let avg_x: f32 = table_x + all_widths[0..9].iter().sum::<f32>();
    let avg_w = all_widths[9];
    let avg_right = avg_x + avg_w;

    // --- Table header ---
    let header_y = y;
    draw_hline(&mut ops, table_x, table_x + total_w, header_y, LINE_THICK);

    // Row 1: group headers
    let groups: Vec<(&str, f32)> = vec![
        ("日付", widths[0]),
        ("始業終業時刻", widths[1] + widths[2]),
        ("拘束時間", widths[3] + widths[4] + widths[5]),
        ("小計", widths[6]),
        ("合計", widths[7]),
        ("拘束\n累計", widths[8]),
        ("運転\n平均", widths[9]),
        ("休息\n時間", widths[10]),
        ("実働", widths[11]),
        ("時間外", widths[12]),
        ("深夜", widths[13]),
        ("時間外\n深夜", widths[14]),
        ("摘要", remarks_w),
    ];
    let mut x = table_x;
    for (text, w) in &groups {
        draw_vline(
            &mut ops,
            x,
            header_y,
            header_y - HEADER_ROW_H * 2.0,
            LINE_THIN,
        );
        // Use first line only for row 1
        let line1 = text.lines().next().unwrap_or("");
        add_text_center_in_cell(
            &mut ops,
            doc,
            font_id,
            x,
            *w,
            header_y - 1.8,
            FONT_SIZE_SMALL,
            line1,
        );
        // Second line if exists
        if let Some(line2) = text.lines().nth(1) {
            add_text_center_in_cell(
                &mut ops,
                doc,
                font_id,
                x,
                *w,
                header_y - HEADER_ROW_H - 0.5,
                FONT_SIZE_SMALL,
                line2,
            );
        }
        x += w;
    }
    draw_vline(
        &mut ops,
        x,
        header_y,
        header_y - HEADER_ROW_H * 2.0,
        LINE_THIN,
    );

    y = header_y - HEADER_ROW_H;
    // ヘッダー中間線: col 8(拘束累計), 9(運転平均), 10(休息時間)をスキップ
    let skip_left: f32 = table_x + all_widths[0..8].iter().sum::<f32>();
    let skip_right: f32 = skip_left + all_widths[8] + all_widths[9] + all_widths[10];
    draw_hline(&mut ops, table_x, skip_left, y, LINE_THIN);
    draw_hline(&mut ops, skip_right, table_x + total_w, y, LINE_THIN);

    // Row 2: sub-headers
    let row2: Vec<(&str, f32)> = vec![
        ("", widths[0]),
        ("始業", widths[1]),
        ("終業", widths[2]),
        ("運転", widths[3]),
        ("荷役", widths[4]),
        ("休憩", widths[5]),
        ("", widths[6]),
        ("", widths[7]),
        ("", widths[8]),
        ("", widths[9]),
        ("", widths[10]),
        ("", widths[11]),
        ("", widths[12]),
        ("", widths[13]),
        ("", widths[14]),
        ("", remarks_w),
    ];
    x = table_x;
    for (text, w) in &row2 {
        draw_vline(&mut ops, x, y, y - HEADER_ROW_H, LINE_THIN);
        if !text.is_empty() {
            add_text_center_in_cell(
                &mut ops,
                doc,
                font_id,
                x,
                *w,
                y - 1.5,
                FONT_SIZE_SMALL,
                text,
            );
        }
        x += w;
    }
    draw_vline(&mut ops, x, y, y - HEADER_ROW_H, LINE_THIN);

    y -= HEADER_ROW_H;
    draw_hline(&mut ops, table_x, table_x + total_w, y, LINE_THICK);

    // --- Day rows ---

    let mut avg_midpoints: Vec<f32> = Vec::new();
    let mut avg_texts: Vec<String> = Vec::new();

    for day in &report.days {
        let weekly_sub = report
            .weekly_subtotals
            .iter()
            .find(|ws| ws.week_end_date == day.date);

        let row_top = y;

        if day.is_holiday {
            draw_day_row_holiday(&mut ops, doc, font_id, table_x, y, &all_widths, day);
            y -= ROW_H_SINGLE;
            draw_hline(&mut ops, table_x, table_x + total_w, y, LINE_THIN);
            avg_midpoints.push((row_top + y) / 2.0);
            avg_texts.push(String::new());
        } else {
            let rh = day_row_height(day);
            draw_day_row(&mut ops, doc, font_id, table_x, y, &all_widths, day, rh);
            y -= rh;
            draw_hline(&mut ops, table_x, table_x + total_w, y, LINE_THIN);
            avg_midpoints.push((row_top + y) / 2.0);
            if let Some(after) = day.drive_avg_after {
                avg_texts.push(fmt_minutes(after));
            } else {
                avg_texts.push(String::new());
            }
        }

        // 小計
        if let Some(ws) = weekly_sub {
            draw_weekly_subtotal(&mut ops, doc, font_id, table_x, y, &all_widths, ws);
            y -= SUBTOTAL_ROW_H;
            draw_hline(&mut ops, table_x, table_x + total_w, y, LINE_THIN);
            avg_midpoints.push((y + y + SUBTOTAL_ROW_H) / 2.0); // 小計中央
            avg_texts.push(String::new());
        }
    }

    let table_body_bottom = y;

    // --- Monthly total row ---
    draw_hline(&mut ops, table_x, table_x + total_w, y, LINE_THICK);
    let monthly_row_h = if report.monthly_total.overlap_restraint_minutes > 0 {
        ROW_H
    } else {
        SUBTOTAL_ROW_H
    };
    draw_monthly_total(
        &mut ops,
        doc,
        font_id,
        table_x,
        y,
        &all_widths,
        &report.monthly_total,
    );
    y -= monthly_row_h;
    draw_hline(&mut ops, table_x, table_x + total_w, y, LINE_THICK);

    // --- 運転平均列 ---
    let avg_grid_top = header_y - HEADER_ROW_H * 1.5;

    // 白塗り
    draw_rect_fill(
        &mut ops,
        avg_x,
        table_body_bottom,
        avg_w,
        avg_grid_top - table_body_bottom,
        1.0,
        1.0,
        1.0,
    );

    // 枠線
    draw_vline(&mut ops, avg_x, header_y, table_body_bottom, LINE_THIN);
    draw_vline(&mut ops, avg_right, header_y, table_body_bottom, LINE_THIN);

    // before を先頭に追加
    let before_text = report
        .days
        .iter()
        .find(|d| !d.is_holiday)
        .and_then(|d| d.drive_avg_before)
        .map(fmt_minutes)
        .unwrap_or_default();
    avg_midpoints.insert(0, avg_grid_top);
    avg_texts.insert(0, before_text);

    // 横罫線 + テキスト
    for i in 0..avg_midpoints.len() {
        draw_hline(&mut ops, avg_x, avg_right, avg_midpoints[i], LINE_THIN);
        if i < avg_texts.len() && !avg_texts[i].is_empty() {
            add_text_center_in_cell(
                &mut ops,
                doc,
                font_id,
                avg_x,
                avg_w,
                avg_midpoints[i] - 1.5,
                FONT_SIZE_BODY,
                &avg_texts[i],
            );
        }
    }

    // --- Footer ---
    y -= 3.0;
    let fiscal_cum = fmt_minutes(report.monthly_total.fiscal_year_cumulative_minutes);
    let month_total_str = fmt_minutes(report.monthly_total.restraint_minutes);
    let year_total = fmt_minutes(report.monthly_total.fiscal_year_total_minutes);

    let reiwa_fy = if month >= 4 {
        reiwa_year
    } else {
        reiwa_year - 1
    };
    let fc = if fiscal_cum.is_empty() {
        "0:00"
    } else {
        &fiscal_cum
    };
    let yt = if year_total.is_empty() {
        "0:00"
    } else {
        &year_total
    };

    add_text(
        &mut ops,
        doc,
        font_id,
        MARGIN_LEFT,
        y,
        FONT_SIZE_SMALL,
        &format!("4月〜前月 累計拘束時間　　{}", fc),
    );
    let mt = if month_total_str.is_empty() {
        "0:00"
    } else {
        &month_total_str
    };
    add_text(
        &mut ops,
        doc,
        font_id,
        MARGIN_LEFT + 80.0,
        y,
        FONT_SIZE_SMALL,
        mt,
    );
    y -= 3.5;
    add_text(
        &mut ops,
        doc,
        font_id,
        MARGIN_LEFT,
        y,
        FONT_SIZE_SMALL,
        &format!("{}年度　拘束時間　　{} 時間", reiwa_fy + 2018, yt),
    );

    add_text_right(
        &mut ops,
        doc,
        font_id,
        PAGE_W - MARGIN_RIGHT,
        y,
        FONT_SIZE_SMALL,
        "大石運輸倉庫　株式会社",
    );

    y -= 4.0;
    add_text(
        &mut ops,
        doc,
        font_id,
        PAGE_W - MARGIN_RIGHT - 90.0,
        y,
        FONT_SIZE_SMALL,
        "* : 2マン運行",
    );
    y -= 3.0;
    add_text(
        &mut ops,
        doc,
        font_id,
        PAGE_W - MARGIN_RIGHT - 90.0,
        y,
        FONT_SIZE_SMALL,
        "D2 : 2分割休息　D3 : 3分割休息",
    );
    y -= 3.0;
    add_text(
        &mut ops,
        doc,
        font_id,
        PAGE_W - MARGIN_RIGHT - 90.0,
        y,
        FONT_SIZE_SMALL,
        "[宿泊を伴う長距離貨物運送]の例外",
    );
    y -= 3.0;
    add_text(
        &mut ops,
        doc,
        font_id,
        PAGE_W - MARGIN_RIGHT - 90.0,
        y,
        FONT_SIZE_SMALL,
        " W16 : 拘束時間延長　R8 : 住所外地休息　R12 : 住所地休息",
    );

    PdfPage::new(Mm(PAGE_W), Mm(PAGE_H), ops)
}

// ===== Day row drawing =====

fn draw_day_row_holiday(
    ops: &mut Vec<Op>,
    doc: &PdfDocument,
    font_id: &FontId,
    x: f32,
    y: f32,
    all_widths: &[f32],
    day: &RestraintDayRow,
) {
    let h = ROW_H_SINGLE;
    let date_str = format!("{}/{}", day.date.month(), day.date.day());
    let mut cx = x;

    draw_vline(ops, cx, y, y - h, LINE_THIN);
    add_text_center_in_cell(
        ops,
        doc,
        font_id,
        cx,
        all_widths[0],
        y - 1.5,
        FONT_SIZE_BODY,
        &date_str,
    );
    cx += all_widths[0];

    // "休" in start column
    draw_vline(ops, cx, y, y - h, LINE_THIN);
    add_text_center_color(
        ops,
        doc,
        font_id,
        cx,
        all_widths[1],
        y - 1.5,
        FONT_SIZE_BODY,
        "休",
        0.8,
        0.2,
        0.2,
    );
    cx += all_widths[1];

    for w in &all_widths[2..] {
        draw_vline(ops, cx, y, y - h, LINE_THIN);
        cx += w;
    }
    draw_vline(ops, cx, y, y - h, LINE_THIN);
}

#[allow(clippy::too_many_arguments)]
fn draw_day_row(
    ops: &mut Vec<Op>,
    doc: &PdfDocument,
    font_id: &FontId,
    table_x: f32,
    y: f32,
    all_widths: &[f32],
    day: &RestraintDayRow,
    row_h: f32,
) {
    let date_str = format!("{}/{}", day.date.month(), day.date.day());
    let start = day.start_time.as_deref().unwrap_or("");
    let end = day.end_time.as_deref().unwrap_or("");
    let has_overlap = day.overlap_restraint_minutes > 0;
    let fs = FONT_SIZE_BODY;
    let main_y = y - 1.5; // テキストY位置（上段）
    let overlap_y = y - row_h * 0.55; // テキストY位置（下段、重複）

    let mut x = table_x;

    // 0: 日付
    draw_vline(ops, x, y, y - row_h, LINE_THIN);
    add_text_center_in_cell(ops, doc, font_id, x, all_widths[0], main_y, fs, &date_str);
    x += all_widths[0];

    // 1: 始業
    draw_vline(ops, x, y, y - row_h, LINE_THIN);
    add_text_center_in_cell(ops, doc, font_id, x, all_widths[1], main_y, fs, start);
    x += all_widths[1];

    // 2: 終業
    draw_vline(ops, x, y, y - row_h, LINE_THIN);
    add_text_center_in_cell(ops, doc, font_id, x, all_widths[2], main_y, fs, end);
    x += all_widths[2];

    // 3: 運転（主運行）
    draw_vline(ops, x, y, y - row_h, LINE_THIN);
    add_text_right_in_cell(
        ops,
        doc,
        font_id,
        x,
        x + all_widths[3],
        main_y,
        fs,
        &fmt_minutes(day.drive_minutes),
    );
    if has_overlap && day.overlap_drive_minutes > 0 {
        add_text_right_in_cell(
            ops,
            doc,
            font_id,
            x,
            x + all_widths[3],
            overlap_y,
            FONT_SIZE_OVERLAP,
            &fmt_minutes(day.overlap_drive_minutes),
        );
    }
    x += all_widths[3];

    // 4: 荷役
    draw_vline(ops, x, y, y - row_h, LINE_THIN);
    add_text_right_in_cell(
        ops,
        doc,
        font_id,
        x,
        x + all_widths[4],
        main_y,
        fs,
        &fmt_minutes(day.cargo_minutes),
    );
    if has_overlap && day.overlap_cargo_minutes > 0 {
        add_text_right_in_cell(
            ops,
            doc,
            font_id,
            x,
            x + all_widths[4],
            overlap_y,
            FONT_SIZE_OVERLAP,
            &fmt_minutes(day.overlap_cargo_minutes),
        );
    }
    x += all_widths[4];

    // 5: 休憩
    draw_vline(ops, x, y, y - row_h, LINE_THIN);
    add_text_right_in_cell(
        ops,
        doc,
        font_id,
        x,
        x + all_widths[5],
        main_y,
        fs,
        &fmt_minutes(day.break_minutes),
    );
    if has_overlap && day.overlap_break_minutes > 0 {
        add_text_right_in_cell(
            ops,
            doc,
            font_id,
            x,
            x + all_widths[5],
            overlap_y,
            FONT_SIZE_OVERLAP,
            &fmt_minutes(day.overlap_break_minutes),
        );
    }
    x += all_widths[5];

    // 6: 小計（主運行の拘束小計）
    draw_vline(ops, x, y, y - row_h, LINE_THIN);
    add_text_right_in_cell(
        ops,
        doc,
        font_id,
        x,
        x + all_widths[6],
        main_y,
        fs,
        &fmt_minutes(day.restraint_main_minutes),
    );
    if has_overlap {
        add_text_right_in_cell(
            ops,
            doc,
            font_id,
            x,
            x + all_widths[6],
            overlap_y,
            FONT_SIZE_OVERLAP,
            &fmt_minutes(day.overlap_restraint_minutes),
        );
    }
    x += all_widths[6];

    // 7: 合計
    draw_vline(ops, x, y, y - row_h, LINE_THIN);
    add_text_right_in_cell(
        ops,
        doc,
        font_id,
        x,
        x + all_widths[7],
        main_y,
        fs,
        &fmt_minutes(day.restraint_total_minutes),
    );
    x += all_widths[7];

    // 8: 拘束累計
    draw_vline(ops, x, y, y - row_h, LINE_THIN);
    add_text_right_in_cell(
        ops,
        doc,
        font_id,
        x,
        x + all_widths[8],
        main_y,
        fs,
        &fmt_minutes(day.restraint_cumulative_minutes),
    );
    x += all_widths[8];

    // 9: 運転平均（後でオフセットグリッドで上書き）
    draw_vline(ops, x, y, y - row_h, LINE_THIN);
    x += all_widths[9];

    // 10: 休息時間
    draw_vline(ops, x, y, y - row_h, LINE_THIN);
    if let Some(rest) = day.rest_period_minutes {
        add_text_right_in_cell(
            ops,
            doc,
            font_id,
            x,
            x + all_widths[10],
            main_y,
            fs,
            &fmt_minutes(rest),
        );
    }
    x += all_widths[10];

    // 11: 実働
    draw_vline(ops, x, y, y - row_h, LINE_THIN);
    add_text_right_in_cell(
        ops,
        doc,
        font_id,
        x,
        x + all_widths[11],
        main_y,
        fs,
        &fmt_minutes(day.actual_work_minutes),
    );
    x += all_widths[11];

    // 12: 時間外
    draw_vline(ops, x, y, y - row_h, LINE_THIN);
    add_text_right_in_cell(
        ops,
        doc,
        font_id,
        x,
        x + all_widths[12],
        main_y,
        fs,
        &fmt_minutes(day.overtime_minutes),
    );
    x += all_widths[12];

    // 13: 深夜
    draw_vline(ops, x, y, y - row_h, LINE_THIN);
    add_text_right_in_cell(
        ops,
        doc,
        font_id,
        x,
        x + all_widths[13],
        main_y,
        fs,
        &fmt_minutes(day.late_night_minutes),
    );
    x += all_widths[13];

    // 14: 時間外深夜
    draw_vline(ops, x, y, y - row_h, LINE_THIN);
    add_text_right_in_cell(
        ops,
        doc,
        font_id,
        x,
        x + all_widths[14],
        main_y,
        fs,
        &fmt_minutes(day.overtime_late_night_minutes),
    );
    x += all_widths[14];

    // 15: 摘要
    draw_vline(ops, x, y, y - row_h, LINE_THIN);
    if !day.remarks.is_empty() {
        add_text(
            ops,
            doc,
            font_id,
            x + 0.5,
            main_y,
            FONT_SIZE_SMALL,
            &day.remarks,
        );
    }
    x += all_widths[15];

    draw_vline(ops, x, y, y - row_h, LINE_THIN);
}

fn draw_weekly_subtotal(
    ops: &mut Vec<Op>,
    doc: &PdfDocument,
    font_id: &FontId,
    table_x: f32,
    y: f32,
    all_widths: &[f32],
    ws: &WeeklySubtotal,
) {
    let total_w: f32 = all_widths.iter().sum();
    let h = SUBTOTAL_ROW_H;
    let ty = y - 1.3;
    let fs = FONT_SIZE_BODY;

    draw_rect_fill(ops, table_x, y - h, total_w, h, 0.92, 0.95, 1.0);

    let mut x = table_x;

    for (i, w) in all_widths.iter().enumerate() {
        draw_vline(ops, x, y, y - h, LINE_THIN);
        match i {
            3 => add_text_right_in_cell(
                ops,
                doc,
                font_id,
                x,
                x + w,
                ty,
                fs,
                &fmt_minutes(ws.drive_minutes),
            ),
            4 => add_text_right_in_cell(
                ops,
                doc,
                font_id,
                x,
                x + w,
                ty,
                fs,
                &fmt_minutes(ws.cargo_minutes),
            ),
            5 => add_text_right_in_cell(
                ops,
                doc,
                font_id,
                x,
                x + w,
                ty,
                fs,
                &fmt_minutes(ws.break_minutes),
            ),
            6 => add_text_right_in_cell(
                ops,
                doc,
                font_id,
                x,
                x + w,
                ty,
                fs,
                &fmt_minutes(ws.restraint_minutes),
            ),
            _ => {}
        }
        x += w;
    }
    draw_vline(ops, x, y, y - h, LINE_THIN);
}

fn draw_monthly_total(
    ops: &mut Vec<Op>,
    doc: &PdfDocument,
    font_id: &FontId,
    table_x: f32,
    y: f32,
    all_widths: &[f32],
    total: &crate::types::MonthlyTotal,
) {
    let total_w: f32 = all_widths.iter().sum();
    let has_overlap = total.overlap_restraint_minutes > 0;
    let h = if has_overlap { ROW_H } else { SUBTOTAL_ROW_H };
    let ty = y - 1.3;
    let overlap_ty = y - h * 0.55;
    let fs = FONT_SIZE_BODY;

    draw_rect_fill(ops, table_x, y - h, total_w, h, 0.93, 0.93, 0.93);

    let mut x = table_x;

    // "合計" in date column
    draw_vline(ops, x, y, y - h, LINE_THIN);
    add_text_center_in_cell(ops, doc, font_id, x, all_widths[0], ty, fs, "合計");
    x += all_widths[0];

    // Skip start/end
    for w in &all_widths[1..3] {
        draw_vline(ops, x, y, y - h, LINE_THIN);
        x += w;
    }

    // 3: 運転合計
    draw_vline(ops, x, y, y - h, LINE_THIN);
    add_text_right_in_cell(
        ops,
        doc,
        font_id,
        x,
        x + all_widths[3],
        ty,
        fs,
        &fmt_minutes(total.drive_minutes),
    );
    if has_overlap && total.overlap_drive_minutes > 0 {
        add_text_right_in_cell(
            ops,
            doc,
            font_id,
            x,
            x + all_widths[3],
            overlap_ty,
            FONT_SIZE_OVERLAP,
            &fmt_minutes(total.overlap_drive_minutes),
        );
    }
    x += all_widths[3];

    // 4: 荷役合計
    draw_vline(ops, x, y, y - h, LINE_THIN);
    add_text_right_in_cell(
        ops,
        doc,
        font_id,
        x,
        x + all_widths[4],
        ty,
        fs,
        &fmt_minutes(total.cargo_minutes),
    );
    if has_overlap && total.overlap_cargo_minutes > 0 {
        add_text_right_in_cell(
            ops,
            doc,
            font_id,
            x,
            x + all_widths[4],
            overlap_ty,
            FONT_SIZE_OVERLAP,
            &fmt_minutes(total.overlap_cargo_minutes),
        );
    }
    x += all_widths[4];

    // 5: 休憩合計
    draw_vline(ops, x, y, y - h, LINE_THIN);
    add_text_right_in_cell(
        ops,
        doc,
        font_id,
        x,
        x + all_widths[5],
        ty,
        fs,
        &fmt_minutes(total.break_minutes),
    );
    if has_overlap && total.overlap_break_minutes > 0 {
        add_text_right_in_cell(
            ops,
            doc,
            font_id,
            x,
            x + all_widths[5],
            overlap_ty,
            FONT_SIZE_OVERLAP,
            &fmt_minutes(total.overlap_break_minutes),
        );
    }
    x += all_widths[5];

    // 6: 拘束小計合計
    draw_vline(ops, x, y, y - h, LINE_THIN);
    add_text_right_in_cell(
        ops,
        doc,
        font_id,
        x,
        x + all_widths[6],
        ty,
        fs,
        &fmt_minutes(total.restraint_minutes),
    );
    if has_overlap {
        add_text_right_in_cell(
            ops,
            doc,
            font_id,
            x,
            x + all_widths[6],
            overlap_ty,
            FONT_SIZE_OVERLAP,
            &fmt_minutes(total.overlap_restraint_minutes),
        );
    }
    x += all_widths[6];

    for w in &all_widths[7..10] {
        draw_vline(ops, x, y, y - h, LINE_THIN);
        x += w;
    }

    // 10: 休息 (empty in total)
    draw_vline(ops, x, y, y - h, LINE_THIN);
    x += all_widths[10];

    // 11: 実働合計
    draw_vline(ops, x, y, y - h, LINE_THIN);
    add_text_right_in_cell(
        ops,
        doc,
        font_id,
        x,
        x + all_widths[11],
        ty,
        fs,
        &fmt_minutes(total.actual_work_minutes),
    );
    x += all_widths[11];

    // 12: 時間外合計
    draw_vline(ops, x, y, y - h, LINE_THIN);
    add_text_right_in_cell(
        ops,
        doc,
        font_id,
        x,
        x + all_widths[12],
        ty,
        fs,
        &fmt_minutes(total.overtime_minutes),
    );
    x += all_widths[12];

    // 13: 深夜合計
    draw_vline(ops, x, y, y - h, LINE_THIN);
    add_text_right_in_cell(
        ops,
        doc,
        font_id,
        x,
        x + all_widths[13],
        ty,
        fs,
        &fmt_minutes(total.late_night_minutes),
    );
    x += all_widths[13];

    // 14: 時間外深夜合計
    draw_vline(ops, x, y, y - h, LINE_THIN);
    add_text_right_in_cell(
        ops,
        doc,
        font_id,
        x,
        x + all_widths[14],
        ty,
        fs,
        &fmt_minutes(total.overtime_late_night_minutes),
    );
    x += all_widths[14];

    // 15: 摘要
    draw_vline(ops, x, y, y - h, LINE_THIN);
    x += all_widths[15];
    draw_vline(ops, x, y, y - h, LINE_THIN);
}

// ===== Drawing helpers =====

fn add_text(
    ops: &mut Vec<Op>,
    doc: &PdfDocument,
    font_id: &FontId,
    x: f32,
    y: f32,
    size: f32,
    text: &str,
) {
    if text.is_empty() {
        return;
    }
    let options = TextShapingOptions::new(Pt(size));
    if let Some(shaped) = doc.shape_text(text, font_id, &options) {
        ops.extend(shaped.get_ops(Point::new(Mm(x), Mm(y))));
    }
}

#[allow(clippy::too_many_arguments)]
fn add_text_center_in_cell(
    ops: &mut Vec<Op>,
    doc: &PdfDocument,
    font_id: &FontId,
    cell_left: f32,
    cell_width: f32,
    y: f32,
    size: f32,
    text: &str,
) {
    if text.is_empty() {
        return;
    }
    let mut options = TextShapingOptions::new(Pt(size));
    options.max_width = Some(Pt(cell_width / 0.3528));
    options.align = TextAlign::Center;
    if let Some(shaped) = doc.shape_text(text, font_id, &options) {
        ops.extend(shaped.get_ops(Point::new(Mm(cell_left), Mm(y))));
    }
}

#[allow(clippy::too_many_arguments)]
fn add_text_center_color(
    ops: &mut Vec<Op>,
    doc: &PdfDocument,
    font_id: &FontId,
    cell_left: f32,
    cell_width: f32,
    y: f32,
    size: f32,
    text: &str,
    r: f32,
    g: f32,
    b: f32,
) {
    if text.is_empty() {
        return;
    }
    ops.push(Op::SaveGraphicsState);
    ops.push(Op::SetFillColor {
        col: Color::Rgb(Rgb {
            r,
            g,
            b,
            icc_profile: None,
        }),
    });
    add_text_center_in_cell(ops, doc, font_id, cell_left, cell_width, y, size, text);
    ops.push(Op::RestoreGraphicsState);
}

fn add_text_right(
    ops: &mut Vec<Op>,
    doc: &PdfDocument,
    font_id: &FontId,
    right_x: f32,
    y: f32,
    size: f32,
    text: &str,
) {
    if text.is_empty() {
        return;
    }
    let options = TextShapingOptions::new(Pt(size));
    if let Some(shaped) = doc.shape_text(text, font_id, &options) {
        let text_w_mm = shaped.width * 0.3528;
        let x = right_x - text_w_mm;
        ops.extend(shaped.get_ops(Point::new(Mm(x), Mm(y))));
    }
}

#[allow(clippy::too_many_arguments)]
fn add_text_right_in_cell(
    ops: &mut Vec<Op>,
    doc: &PdfDocument,
    font_id: &FontId,
    _cell_left: f32,
    cell_right: f32,
    y: f32,
    size: f32,
    text: &str,
) {
    if text.is_empty() {
        return;
    }
    add_text_right(ops, doc, font_id, cell_right - 0.8, y, size, text);
}

fn lp(x_mm: f32, y_mm: f32) -> LinePoint {
    LinePoint {
        p: Point::new(Mm(x_mm), Mm(y_mm)),
        bezier: false,
    }
}

fn draw_hline(ops: &mut Vec<Op>, x1: f32, x2: f32, y: f32, _thickness: f32) {
    ops.push(Op::SaveGraphicsState);
    ops.push(Op::SetOutlineColor {
        col: Color::Rgb(Rgb {
            r: 0.3,
            g: 0.3,
            b: 0.3,
            icc_profile: None,
        }),
    });
    ops.push(Op::DrawLine {
        line: Line {
            points: vec![lp(x1, y), lp(x2, y)],
            is_closed: false,
        },
    });
    ops.push(Op::RestoreGraphicsState);
}

fn draw_vline(ops: &mut Vec<Op>, x: f32, y1: f32, y2: f32, _thickness: f32) {
    ops.push(Op::SaveGraphicsState);
    ops.push(Op::SetOutlineColor {
        col: Color::Rgb(Rgb {
            r: 0.3,
            g: 0.3,
            b: 0.3,
            icc_profile: None,
        }),
    });
    ops.push(Op::DrawLine {
        line: Line {
            points: vec![lp(x, y1), lp(x, y2)],
            is_closed: false,
        },
    });
    ops.push(Op::RestoreGraphicsState);
}

#[allow(clippy::too_many_arguments)]
fn draw_rect_fill(ops: &mut Vec<Op>, x: f32, y: f32, w: f32, h: f32, r: f32, g: f32, b: f32) {
    ops.push(Op::SaveGraphicsState);
    ops.push(Op::SetFillColor {
        col: Color::Rgb(Rgb {
            r,
            g,
            b,
            icc_profile: None,
        }),
    });
    ops.push(Op::DrawPolygon {
        polygon: Polygon {
            rings: vec![PolygonRing {
                points: vec![lp(x, y), lp(x + w, y), lp(x + w, y + h), lp(x, y + h)],
            }],
            mode: PaintMode::Fill,
            winding_order: WindingOrder::NonZero,
        },
    });
    ops.push(Op::RestoreGraphicsState);
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{MonthlyTotal, OperationDetail};
    use chrono::NaiveDate;
    use uuid::Uuid;

    fn make_workday(date: NaiveDate, drive: i32, remarks: &str) -> RestraintDayRow {
        RestraintDayRow {
            date,
            is_holiday: false,
            start_time: Some("8:00".to_string()),
            end_time: Some("17:00".to_string()),
            operations: vec![OperationDetail {
                unko_no: "001".to_string(),
                drive_minutes: drive,
                cargo_minutes: 30,
                break_minutes: 60,
                restraint_minutes: drive + 90,
            }],
            drive_minutes: drive,
            cargo_minutes: 30,
            break_minutes: 60,
            restraint_total_minutes: drive + 90,
            restraint_cumulative_minutes: drive + 90,
            drive_average_minutes: drive as f64,
            rest_period_minutes: Some(600),
            remarks: remarks.to_string(),
            overlap_drive_minutes: 0,
            overlap_cargo_minutes: 0,
            overlap_break_minutes: 0,
            overlap_restraint_minutes: 0,
            restraint_main_minutes: drive + 90,
            drive_avg_before: Some(drive),
            drive_avg_after: Some(drive),
            actual_work_minutes: drive + 30,
            overtime_minutes: 60,
            late_night_minutes: 30,
            overtime_late_night_minutes: 15,
        }
    }

    fn make_monthly_total(fiscal_cum: i32) -> MonthlyTotal {
        MonthlyTotal {
            drive_minutes: 480,
            cargo_minutes: 120,
            break_minutes: 240,
            restraint_minutes: 840,
            fiscal_year_cumulative_minutes: fiscal_cum,
            fiscal_year_total_minutes: fiscal_cum + 840,
            overlap_drive_minutes: 0,
            overlap_cargo_minutes: 0,
            overlap_break_minutes: 0,
            overlap_restraint_minutes: 0,
            actual_work_minutes: 600,
            overtime_minutes: 120,
            late_night_minutes: 60,
            overtime_late_night_minutes: 30,
        }
    }

    fn make_report(days: Vec<RestraintDayRow>, fiscal_cum: i32) -> RestraintReportResponse {
        RestraintReportResponse {
            driver_id: Uuid::new_v4(),
            driver_name: "テスト太郎".to_string(),
            year: 2026,
            month: 3,
            max_restraint_minutes: 16500,
            days,
            weekly_subtotals: Vec::new(),
            monthly_total: make_monthly_total(fiscal_cum),
        }
    }

    // L592: drive_avg_after が None の非休日行
    #[test]
    fn test_generate_pdf_drive_avg_after_none() {
        let mut day = make_workday(NaiveDate::from_ymd_opt(2026, 3, 2).unwrap(), 120, "");
        day.drive_avg_after = None;
        let report = make_report(vec![day], 0);
        let result = generate_pdf(&[report], &["CD01".into()], 2026, 3);
        assert!(!result.is_empty());
    }

    // L688: fiscal_year_cumulative_minutes > 0
    #[test]
    fn test_generate_pdf_fiscal_cumulative_nonzero() {
        let day = make_workday(NaiveDate::from_ymd_opt(2026, 3, 2).unwrap(), 120, "");
        let report = make_report(vec![day], 5000);
        let result = generate_pdf(&[report], &["CD02".into()], 2026, 3);
        assert!(!result.is_empty());
    }

    // L1083-1091: remarks が非空
    #[test]
    fn test_generate_pdf_nonempty_remarks() {
        let day = make_workday(
            NaiveDate::from_ymd_opt(2026, 3, 2).unwrap(),
            120,
            "出発:テスト地点",
        );
        let report = make_report(vec![day], 0);
        let result = generate_pdf(&[report], &["CD03".into()], 2026, 3);
        assert!(!result.is_empty());
    }

    // L1384: add_text の空テキスト早期リターン
    #[test]
    fn test_add_text_empty() {
        let mut doc = PdfDocument::new("test");
        let mut warnings = Vec::new();
        let font = ParsedFont::from_bytes(FONT_DATA, 0, &mut warnings).unwrap();
        let font_id = doc.add_font(&font);
        let mut ops = Vec::new();
        add_text(&mut ops, &doc, &font_id, 0.0, 10.0, 8.0, "");
        assert!(ops.is_empty());
    }

    // L1404: add_text_center_in_cell の空テキスト早期リターン
    #[test]
    fn test_add_text_center_in_cell_empty() {
        let mut doc = PdfDocument::new("test");
        let mut warnings = Vec::new();
        let font = ParsedFont::from_bytes(FONT_DATA, 0, &mut warnings).unwrap();
        let font_id = doc.add_font(&font);
        let mut ops = Vec::new();
        add_text_center_in_cell(&mut ops, &doc, &font_id, 0.0, 10.0, 10.0, 8.0, "");
        assert!(ops.is_empty());
    }

    // L1429: add_text_center_color の空テキスト早期リターン
    #[test]
    fn test_add_text_center_color_empty() {
        let mut doc = PdfDocument::new("test");
        let mut warnings = Vec::new();
        let font = ParsedFont::from_bytes(FONT_DATA, 0, &mut warnings).unwrap();
        let font_id = doc.add_font(&font);
        let mut ops = Vec::new();
        add_text_center_color(
            &mut ops, &doc, &font_id, 0.0, 10.0, 10.0, 8.0, "", 1.0, 0.0, 0.0,
        );
        assert!(ops.is_empty());
    }

    // L1454: add_text_right の空テキスト早期リターン
    #[test]
    fn test_add_text_right_empty() {
        let mut doc = PdfDocument::new("test");
        let mut warnings = Vec::new();
        let font = ParsedFont::from_bytes(FONT_DATA, 0, &mut warnings).unwrap();
        let font_id = doc.add_font(&font);
        let mut ops = Vec::new();
        add_text_right(&mut ops, &doc, &font_id, 50.0, 10.0, 8.0, "");
        assert!(ops.is_empty());
    }

    // overlap 分岐: day の overlap_*_minutes > 0 + total の overlap
    #[test]
    fn test_generate_pdf_with_overlap() {
        let mut day = make_workday(NaiveDate::from_ymd_opt(2026, 3, 2).unwrap(), 120, "");
        day.overlap_drive_minutes = 30;
        day.overlap_cargo_minutes = 15;
        day.overlap_break_minutes = 10;
        day.overlap_restraint_minutes = 55;
        let mut report = make_report(vec![day], 0);
        report.monthly_total.overlap_drive_minutes = 30;
        report.monthly_total.overlap_cargo_minutes = 15;
        report.monthly_total.overlap_break_minutes = 10;
        report.monthly_total.overlap_restraint_minutes = 55;
        let result = generate_pdf(&[report], &["CD04".into()], 2026, 3);
        assert!(!result.is_empty());
    }

    // weekly_subtotals が非空
    #[test]
    fn test_generate_pdf_with_weekly_subtotals() {
        let day = make_workday(NaiveDate::from_ymd_opt(2026, 3, 7).unwrap(), 120, "");
        let mut report = make_report(vec![day], 0);
        report.weekly_subtotals = vec![WeeklySubtotal {
            week_end_date: NaiveDate::from_ymd_opt(2026, 3, 7).unwrap(),
            drive_minutes: 600,
            cargo_minutes: 120,
            break_minutes: 300,
            restraint_minutes: 1020,
        }];
        let result = generate_pdf(&[report], &["CD05".into()], 2026, 3);
        assert!(!result.is_empty());
    }

    // month >= 4 (令和年度が reiwa_year になる分岐)
    #[test]
    fn test_generate_pdf_month_april() {
        let day = make_workday(NaiveDate::from_ymd_opt(2026, 4, 1).unwrap(), 120, "");
        let report = make_report(vec![day], 0);
        let result = generate_pdf(&[report], &["CD06".into()], 2026, 4);
        assert!(!result.is_empty());
    }
}
