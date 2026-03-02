use std::collections::VecDeque;
use std::path::Path;
use std::{cmp::Ordering, collections::BTreeMap};

use eframe::egui;

use crate::dicom::DicomImage;

pub fn normalize_token(value: Option<&str>) -> String {
    value
        .unwrap_or_default()
        .trim()
        .to_ascii_uppercase()
        .replace(' ', "")
}

pub fn classify_laterality(value: Option<&str>) -> Option<&'static str> {
    let token = normalize_token(value);
    if token.starts_with('R') || token.contains("RIGHT") {
        Some("R")
    } else if token.starts_with('L') || token.contains("LEFT") {
        Some("L")
    } else {
        None
    }
}

pub fn classify_view(value: Option<&str>) -> Option<&'static str> {
    let token = normalize_token(value);
    if token == "MLO" {
        Some("MLO")
    } else if token == "CC" {
        Some("CC")
    } else {
        None
    }
}

fn mammo_slot_index(image: &DicomImage) -> Option<usize> {
    match (
        classify_view(image.view_position.as_deref()),
        classify_laterality(image.image_laterality.as_deref()),
    ) {
        (Some("CC"), Some("R")) => Some(0),
        (Some("CC"), Some("L")) => Some(1),
        (Some("MLO"), Some("R")) => Some(2),
        (Some("MLO"), Some("L")) => Some(3),
        _ => None,
    }
}

fn three_up_sort_key(image: &DicomImage) -> (u8, u8, i32) {
    let view_rank = match classify_view(image.view_position.as_deref()) {
        Some("CC") => 0,
        Some("MLO") => 1,
        _ => 2,
    };
    let laterality_rank = match classify_laterality(image.image_laterality.as_deref()) {
        Some("R") => 0,
        Some("L") => 1,
        _ => 2,
    };
    let instance_number = image.instance_number.unwrap_or(i32::MAX);
    (view_rank, laterality_rank, instance_number)
}

fn study_date_value(image: &DicomImage) -> Option<String> {
    image
        .metadata
        .iter()
        .find(|(key, _)| key == "StudyDate")
        .map(|(_, value)| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn sort_quartet_indices(indices: &mut [usize], entries: &[(u8, u8, i32)]) {
    indices.sort_by_key(|index| (entries[*index], *index));
}

fn compare_study_dates_desc(a: Option<&str>, b: Option<&str>) -> Ordering {
    match (a, b) {
        (Some(a), Some(b)) => b.cmp(a),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

fn order_eight_up_indices(entries: &[(u8, u8, i32)], study_dates: &[Option<String>]) -> Vec<usize> {
    let mut by_study = BTreeMap::<Option<String>, Vec<usize>>::new();
    for (index, study_date) in study_dates.iter().take(entries.len()).enumerate() {
        by_study
            .entry(study_date.clone())
            .or_default()
            .push(index);
    }

    let mut grouped = by_study.into_iter().collect::<Vec<_>>();
    if grouped.len() == 2 && grouped.iter().all(|(_, indices)| indices.len() == 4) {
        grouped.sort_by(|(study_a, _), (study_b, _)| {
            compare_study_dates_desc(study_a.as_deref(), study_b.as_deref())
        });
        let mut ordered = Vec::with_capacity(8);
        for (_, mut indices) in grouped {
            sort_quartet_indices(&mut indices, entries);
            ordered.extend(indices);
        }
        return ordered;
    }

    // Fallback: keep row assignment from incoming order (first 4 + last 4),
    // but enforce canonical RCC/LCC/RMLO/LMLO ordering inside each row.
    let mut first_row = (0..4).collect::<Vec<_>>();
    let mut second_row = (4..8).collect::<Vec<_>>();
    sort_quartet_indices(&mut first_row, entries);
    sort_quartet_indices(&mut second_row, entries);
    first_row.into_iter().chain(second_row).collect()
}

pub fn preferred_mammo_slot(
    image: &DicomImage,
    len: usize,
    mut is_free: impl FnMut(usize) -> bool,
) -> Option<usize> {
    let laterality = classify_laterality(image.image_laterality.as_deref());
    mammo_slot_index(image)
        .filter(|index| *index < len && is_free(*index))
        .or_else(|| {
            preferred_slots_for_laterality(laterality)
                .into_iter()
                .find(|index| *index < len && is_free(*index))
        })
}

pub fn order_mammo_indices<T>(
    items: &[T],
    mut image_of: impl FnMut(&T) -> &DicomImage,
) -> Vec<usize> {
    if items.len() == 8 {
        let mut keys = Vec::with_capacity(items.len());
        let mut study_dates = Vec::with_capacity(items.len());
        for item in items {
            let image = image_of(item);
            keys.push(three_up_sort_key(image));
            study_dates.push(study_date_value(image));
        }
        return order_eight_up_indices(&keys, &study_dates);
    }

    if items.len() == 3 {
        let mut keyed = items
            .iter()
            .enumerate()
            .map(|(index, item)| (index, three_up_sort_key(image_of(item))))
            .collect::<Vec<_>>();
        keyed.sort_by_key(|(index, key)| (*key, *index));
        return keyed.into_iter().map(|(index, _)| index).collect();
    }

    let mut ordered = vec![None; items.len()];
    let mut fallback = VecDeque::new();

    for (index, item) in items.iter().enumerate() {
        let image = image_of(item);
        let slot = preferred_mammo_slot(image, ordered.len(), |slot_index| {
            ordered.get(slot_index).and_then(Option::as_ref).is_none()
        });

        if let Some(slot) = slot {
            if ordered[slot].is_none() {
                ordered[slot] = Some(index);
            } else {
                fallback.push_back(index);
            }
        } else {
            fallback.push_back(index);
        }
    }

    for slot in ordered.iter_mut() {
        if slot.is_none() {
            *slot = fallback.pop_front();
        }
    }

    ordered.into_iter().flatten().collect()
}

pub fn preferred_slots_for_laterality(laterality: Option<&str>) -> [usize; 4] {
    match laterality {
        Some("R") => [0, 2, 1, 3],
        Some("L") => [1, 3, 0, 2],
        _ => [0, 1, 2, 3],
    }
}

pub fn mammo_image_align(index: usize) -> egui::Align {
    match index % 4 {
        // Quadrants 1 and 3 (left column): right-justify image in viewport.
        0 | 2 => egui::Align::Max,
        // Quadrants 2 and 4 (right column): left-justify image in viewport.
        1 | 3 => egui::Align::Min,
        _ => egui::Align::Center,
    }
}

pub fn mammo_label(image: &DicomImage, path: &Path) -> String {
    let laterality = classify_laterality(image.image_laterality.as_deref());
    let view = classify_view(image.view_position.as_deref());
    let code = match (laterality, view) {
        (Some(laterality), Some(view)) => format!("{laterality}{view}"),
        (Some(laterality), None) => laterality.to_string(),
        (None, Some(view)) => view.to_string(),
        _ => String::new(),
    };

    let file_name = path
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("DICOM");

    if code.is_empty() {
        file_name.to_string()
    } else {
        format!("{code} ({file_name})")
    }
}
