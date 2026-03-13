use std::collections::BTreeMap;

use anyhow::{bail, Context, Result};
use reqwest::blocking::Client;
use reqwest::header::ACCEPT;

use crate::dicom::{
    dicom_identity_key_from_parts, dicom_source_from_bytes_with_identity, is_gsps_sop_class_uid,
    is_structured_report_sop_class_uid, DicomPathKind, DicomSource,
};
use crate::launch::{DicomWebGroupedLaunchRequest, DicomWebLaunchRequest};
use crate::mammo::{classify_laterality, classify_view};

const TAG_SOP_CLASS_UID: &str = "00080016";
const TAG_SOP_INSTANCE_UID: &str = "00080018";
const TAG_MODALITY: &str = "00080060";
const TAG_SERIES_INSTANCE_UID: &str = "0020000E";
const TAG_INSTANCE_NUMBER: &str = "00200013";
const TAG_VIEW_POSITION: &str = "00185101";
const TAG_IMAGE_LATERALITY: &str = "00200062";
const TAG_LATERALITY: &str = "00200060";

#[derive(Clone)]
struct MetadataInstance {
    series_uid: Option<String>,
    instance_uid: String,
    sop_class_uid: Option<String>,
    modality: Option<String>,
    view_position: Option<String>,
    laterality: Option<String>,
    instance_number: Option<i32>,
}

#[derive(Clone, Copy)]
struct DownloadInstanceRequest<'a> {
    study_uid: &'a str,
    series_uid: Option<&'a str>,
    sop_class_uid: Option<&'a str>,
    modality: Option<&'a str>,
    instance_uid: &'a str,
}

#[derive(Debug, Clone)]
pub enum DicomWebDownloadResult {
    Single(Vec<DicomSource>),
    Grouped {
        groups: Vec<Vec<DicomSource>>,
        open_group: usize,
    },
}

#[derive(Debug, Clone)]
pub enum DicomWebGroupStreamUpdate {
    ActiveGroupInstanceCount(usize),
    ActivePath(DicomSource),
}

pub fn download_dicomweb_request(
    request: &DicomWebLaunchRequest,
) -> Result<DicomWebDownloadResult> {
    let client = build_http_client()?;
    let base = normalize_base_url(&request.base_url);
    let auth = request.username.as_deref().zip(request.password.as_deref());

    if let Some(instance_uid) = request.instance_uid.as_ref() {
        let path = download_instance(
            &client,
            &base,
            DownloadInstanceRequest {
                study_uid: &request.study_uid,
                series_uid: request.series_uid.as_deref(),
                sop_class_uid: None,
                modality: None,
                instance_uid,
            },
            auth,
        )?;
        return Ok(DicomWebDownloadResult::Single(vec![path]));
    }

    let metadata_instances = fetch_instance_metadata(
        &client,
        &base,
        &request.study_uid,
        request.series_uid.as_deref(),
        auth,
    )?;
    if metadata_instances.is_empty() {
        bail!("DICOMweb metadata query returned no instances");
    }

    let selected = select_instances_for_viewer(metadata_instances, request.series_uid.as_deref())?;
    let paths = download_instances_parallel(&client, &base, &request.study_uid, auth, &selected)?;

    Ok(DicomWebDownloadResult::Single(paths))
}

pub fn download_dicomweb_group_request<F>(
    request: &DicomWebGroupedLaunchRequest,
    mut on_active_path: F,
) -> Result<DicomWebDownloadResult>
where
    F: FnMut(DicomWebGroupStreamUpdate),
{
    let client = build_http_client()?;
    let base = normalize_base_url(&request.base_url);
    let auth = request.username.as_deref().zip(request.password.as_deref());

    if request.groups.is_empty() {
        bail!("DICOMweb grouped launch requested no groups");
    }

    let open_group = request
        .open_group
        .min(request.groups.len().saturating_sub(1));

    let mut downloaded_groups = (0..request.groups.len())
        .map(|_| None::<Vec<DicomSource>>)
        .collect::<Vec<_>>();

    let active_group_instances = resolve_group_instances(
        &client,
        &base,
        &request.study_uid,
        auth,
        open_group,
        &request.groups[open_group],
    )?;

    if let Some(count) = active_group_instance_count(&active_group_instances) {
        on_active_path(DicomWebGroupStreamUpdate::ActiveGroupInstanceCount(count));
    }
    downloaded_groups[open_group] = Some(download_instances_streaming(
        &client,
        &base,
        &request.study_uid,
        auth,
        &active_group_instances,
        &mut on_active_path,
    )?);

    for group_index in ordered_group_indices(request.groups.len(), open_group)
        .into_iter()
        .skip(1)
    {
        let selected_instances = resolve_group_instances(
            &client,
            &base,
            &request.study_uid,
            auth,
            group_index,
            &request.groups[group_index],
        )?;
        let group_paths = download_instances_parallel(
            &client,
            &base,
            &request.study_uid,
            auth,
            &selected_instances,
        )?;
        downloaded_groups[group_index] = Some(group_paths);
    }

    let downloaded_groups = downloaded_groups
        .into_iter()
        .enumerate()
        .map(|(group_index, group)| {
            group.ok_or_else(|| {
                anyhow::anyhow!(
                    "DICOMweb group {} failed to produce DicomSource values",
                    group_index
                )
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(DicomWebDownloadResult::Grouped {
        groups: downloaded_groups,
        open_group,
    })
}

fn resolve_group_instances(
    client: &Client,
    base: &str,
    study_uid: &str,
    auth: Option<(&str, &str)>,
    group_index: usize,
    group_series_uids: &[String],
) -> Result<Vec<MetadataInstance>> {
    if group_series_uids.is_empty() {
        bail!(
            "DICOMweb group {} did not include any series UIDs",
            group_index
        );
    }

    let mut reduced_by_series = Vec::<Vec<MetadataInstance>>::new();

    for series_uid in group_series_uids {
        let metadata_instances =
            fetch_instance_metadata(client, base, study_uid, Some(series_uid.as_str()), auth)
                .with_context(|| {
                    format!(
                        "Failed fetching DICOMweb metadata for group {} series {}",
                        group_index, series_uid
                    )
                })?;

        if metadata_instances.is_empty() {
            bail!(
                "Group {} series {} returned no DICOM instances",
                group_index,
                series_uid
            );
        }

        let reduced = reduce_series_instances(metadata_instances).with_context(|| {
            format!(
                "Group {} series {} did not resolve to a supported instance set",
                group_index, series_uid
            )
        })?;
        reduced_by_series.push(reduced);
    }

    let selected_instances = select_group_instances_from_reduced_sets(reduced_by_series);
    let displayable_image_count = displayable_group_image_count(&selected_instances);

    if displayable_image_count > 0 && !matches!(displayable_image_count, 1..=4 | 8) {
        bail!(
            "DICOMweb group {} resolved to {} displayable image instances; each group must resolve to 1, 2, 3, 4, or 8 displayable images",
            group_index,
            displayable_image_count
        );
    }
    if displayable_image_count == 0 && !has_displayable_group_content(&selected_instances) {
        bail!(
            "DICOMweb group {} resolved to no displayable images or structured reports",
            group_index
        );
    }

    Ok(selected_instances)
}

fn ordered_group_indices(group_count: usize, open_group: usize) -> Vec<usize> {
    if group_count == 0 {
        return Vec::new();
    }

    let open_group = open_group.min(group_count.saturating_sub(1));
    let mut indices = Vec::with_capacity(group_count);
    indices.push(open_group);
    indices.extend((0..group_count).filter(|group_index| *group_index != open_group));
    indices
}

fn select_group_instances_from_reduced_sets(
    reduced_by_series: Vec<Vec<MetadataInstance>>,
) -> Vec<MetadataInstance> {
    let mut selected_instances = Vec::<MetadataInstance>::new();
    if reduced_by_series.len() == 1 {
        if let Some(mut reduced) = reduced_by_series.into_iter().next() {
            selected_instances.append(&mut reduced);
        }
    } else if reduced_by_series.len() == 2
        && reduced_by_series.iter().all(|reduced| reduced.len() == 4)
    {
        for mut reduced in reduced_by_series {
            selected_instances.append(&mut reduced);
        }
    } else {
        for reduced in reduced_by_series {
            if let Some(first) = reduced.into_iter().next() {
                selected_instances.push(first);
            }
        }
    }
    selected_instances
}

fn modality_indicates_image(modality: &str) -> bool {
    matches!(
        modality.to_ascii_uppercase().as_str(),
        "CR" | "CT"
            | "DX"
            | "IO"
            | "MG"
            | "MR"
            | "NM"
            | "OT"
            | "PT"
            | "RF"
            | "RG"
            | "RTIMAGE"
            | "SC"
            | "US"
            | "XA"
            | "XC"
    )
}

fn metadata_instance_kind(instance: &MetadataInstance) -> DicomPathKind {
    if instance
        .sop_class_uid
        .as_deref()
        .is_some_and(is_gsps_sop_class_uid)
    {
        return DicomPathKind::Gsps;
    }
    if instance
        .sop_class_uid
        .as_deref()
        .is_some_and(is_structured_report_sop_class_uid)
        || instance
            .modality
            .as_deref()
            .is_some_and(|value| value.eq_ignore_ascii_case("SR"))
    {
        return DicomPathKind::StructuredReport;
    }
    if instance
        .modality
        .as_deref()
        .is_some_and(modality_indicates_image)
    {
        return DicomPathKind::Image;
    }
    DicomPathKind::Other
}

fn displayable_group_image_count(instances: &[MetadataInstance]) -> usize {
    instances
        .iter()
        .filter(|instance| metadata_instance_kind(instance) == DicomPathKind::Image)
        .count()
}

fn has_displayable_group_content(instances: &[MetadataInstance]) -> bool {
    instances.iter().any(|instance| {
        matches!(
            metadata_instance_kind(instance),
            DicomPathKind::Image | DicomPathKind::StructuredReport
        )
    })
}

fn active_group_instance_count(instances: &[MetadataInstance]) -> Option<usize> {
    let image_count = displayable_group_image_count(instances);
    (image_count > 0).then_some(image_count)
}

fn download_instances_streaming<F>(
    client: &Client,
    base: &str,
    study_uid: &str,
    auth: Option<(&str, &str)>,
    instances: &[MetadataInstance],
    on_path: &mut F,
) -> Result<Vec<DicomSource>>
where
    F: FnMut(DicomWebGroupStreamUpdate),
{
    download_instances_streaming_with(instances, on_path, |instance| {
        download_instance(
            client,
            base,
            DownloadInstanceRequest {
                study_uid,
                series_uid: instance.series_uid.as_deref(),
                sop_class_uid: instance.sop_class_uid.as_deref(),
                modality: instance.modality.as_deref(),
                instance_uid: &instance.instance_uid,
            },
            auth,
        )
    })
}

fn download_instances_streaming_with<F, D>(
    instances: &[MetadataInstance],
    on_path: &mut F,
    mut downloader: D,
) -> Result<Vec<DicomSource>>
where
    F: FnMut(DicomWebGroupStreamUpdate),
    D: FnMut(&MetadataInstance) -> Result<DicomSource>,
{
    let mut paths = Vec::with_capacity(instances.len());
    for instance in instances {
        let path = downloader(instance)?;
        on_path(DicomWebGroupStreamUpdate::ActivePath(path.clone()));
        paths.push(path);
    }
    Ok(paths)
}

fn build_http_client() -> Result<Client> {
    Client::builder()
        .connect_timeout(std::time::Duration::from_secs(10))
        .timeout(std::time::Duration::from_secs(120))
        .build()
        .context("Could not initialize HTTP client for DICOMweb")
}

fn normalize_base_url(base_url: &str) -> String {
    let trimmed = strip_query_and_fragment(base_url.trim())
        .trim()
        .trim_end_matches('/');
    if trimmed.is_empty() {
        return String::new();
    }

    if has_root_only_path(trimmed) {
        return format!("{trimmed}/dicom-web");
    }

    trimmed.to_string()
}

fn strip_query_and_fragment(value: &str) -> &str {
    let query_index = value.find('?').unwrap_or(value.len());
    let fragment_index = value.find('#').unwrap_or(value.len());
    &value[..query_index.min(fragment_index)]
}

fn has_root_only_path(url: &str) -> bool {
    if let Some((_, rest)) = url.split_once("://") {
        match rest.find('/') {
            None => true,
            Some(path_index) => rest[path_index..].trim_matches('/').is_empty(),
        }
    } else {
        !url.contains('/')
    }
}

fn fetch_instance_metadata(
    client: &Client,
    base: &str,
    study_uid: &str,
    series_uid: Option<&str>,
    auth: Option<(&str, &str)>,
) -> Result<Vec<MetadataInstance>> {
    let url = metadata_url(base, study_uid, series_uid);

    let metadata_json = http_get_text(client, &url, "application/dicom+json", auth)
        .with_context(|| format!("Failed fetching DICOMweb metadata from {url}"))?;
    parse_metadata_instances(&metadata_json)
}

fn metadata_url(base: &str, study_uid: &str, series_uid: Option<&str>) -> String {
    if let Some(series_uid) = series_uid {
        format!("{base}/studies/{study_uid}/series/{series_uid}/metadata")
    } else {
        format!("{base}/studies/{study_uid}/metadata")
    }
}

fn parse_metadata_instances(json: &str) -> Result<Vec<MetadataInstance>> {
    let object_slices = split_top_level_json_objects(json)
        .with_context(|| "DICOMweb metadata JSON parsing failed".to_string())?;
    let mut instances = Vec::new();

    for obj in object_slices {
        let instance_uid = match first_tag_string(obj, TAG_SOP_INSTANCE_UID) {
            Some(value) if !value.trim().is_empty() => value,
            _ => continue,
        };
        let metadata = MetadataInstance {
            series_uid: first_tag_string(obj, TAG_SERIES_INSTANCE_UID),
            instance_uid,
            sop_class_uid: normalize_metadata_string(first_tag_string(obj, TAG_SOP_CLASS_UID)),
            modality: normalize_metadata_string(first_tag_string(obj, TAG_MODALITY)),
            view_position: first_tag_string(obj, TAG_VIEW_POSITION),
            laterality: first_tag_string(obj, TAG_IMAGE_LATERALITY)
                .or_else(|| first_tag_string(obj, TAG_LATERALITY)),
            instance_number: first_tag_string(obj, TAG_INSTANCE_NUMBER)
                .and_then(|value| value.parse::<i32>().ok()),
        };
        instances.push(metadata);
    }

    Ok(instances)
}

fn normalize_metadata_string(value: Option<String>) -> Option<String> {
    value.and_then(|value| {
        let trimmed = value.trim();
        (!trimmed.is_empty()).then(|| trimmed.to_string())
    })
}

fn split_top_level_json_objects(input: &str) -> Result<Vec<&str>> {
    let mut objects = Vec::new();
    let mut depth = 0usize;
    let mut object_start = None::<usize>;
    let mut in_string = false;
    let mut escaped = false;

    for (index, ch) in input.char_indices() {
        if in_string {
            if escaped {
                escaped = false;
                continue;
            }
            if ch == '\\' {
                escaped = true;
            } else if ch == '"' {
                in_string = false;
            }
            continue;
        }

        match ch {
            '"' => in_string = true,
            '{' => {
                if depth == 0 {
                    object_start = Some(index);
                }
                depth += 1;
            }
            '}' => {
                if depth == 0 {
                    bail!("Unexpected closing brace in JSON");
                }
                depth -= 1;
                if depth == 0 {
                    if let Some(start) = object_start.take() {
                        objects.push(&input[start..=index]);
                    }
                }
            }
            _ => {}
        }
    }

    if depth != 0 || in_string {
        bail!("Unbalanced JSON while parsing metadata");
    }
    Ok(objects)
}

fn first_tag_string(object: &str, tag: &str) -> Option<String> {
    let tag_object = top_level_tag_object_slice(object, tag)?;
    let value_pos = tag_object.find("\"Value\"")?;
    let after_value = &tag_object[value_pos + "\"Value\"".len()..];
    let array_start = after_value.find('[')?;
    let after_array_start = &after_value[array_start + 1..];
    let first_token = parse_first_json_token(after_array_start)?;
    first_token_to_string(first_token)
}

fn top_level_tag_object_slice<'a>(object: &'a str, tag: &str) -> Option<&'a str> {
    let bytes = object.as_bytes();
    let mut depth = 0usize;
    let mut index = 0usize;

    while index < bytes.len() {
        match bytes[index] {
            b'"' => {
                let key_start = index + 1;
                index += 1;
                let mut key_escaped = false;
                while index < bytes.len() {
                    let current = bytes[index];
                    if key_escaped {
                        key_escaped = false;
                    } else if current == b'\\' {
                        key_escaped = true;
                    } else if current == b'"' {
                        break;
                    }
                    index += 1;
                }
                if index >= bytes.len() {
                    return None;
                }

                if depth == 1 && object.get(key_start..index) == Some(tag) {
                    let mut value_index = index + 1;
                    while value_index < bytes.len() && bytes[value_index].is_ascii_whitespace() {
                        value_index += 1;
                    }
                    if bytes.get(value_index) != Some(&b':') {
                        return None;
                    }
                    value_index += 1;
                    while value_index < bytes.len() && bytes[value_index].is_ascii_whitespace() {
                        value_index += 1;
                    }
                    if bytes.get(value_index) != Some(&b'{') {
                        return None;
                    }
                    let value_end = find_matching_object_end(bytes, value_index)?;
                    return object.get(value_index..=value_end);
                }

                index += 1;
            }
            b'{' => {
                depth += 1;
                index += 1;
            }
            b'}' => {
                if depth == 0 {
                    return None;
                }
                depth -= 1;
                index += 1;
            }
            _ => {
                index += 1;
            }
        }
    }

    None
}

fn find_matching_object_end(bytes: &[u8], start: usize) -> Option<usize> {
    if bytes.get(start) != Some(&b'{') {
        return None;
    }

    let mut depth = 0usize;
    let mut in_string = false;
    let mut escaped = false;

    for (index, byte) in bytes.iter().enumerate().skip(start) {
        if in_string {
            if escaped {
                escaped = false;
            } else if *byte == b'\\' {
                escaped = true;
            } else if *byte == b'"' {
                in_string = false;
            }
            continue;
        }

        match *byte {
            b'"' => in_string = true,
            b'{' => depth += 1,
            b'}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(index);
                }
            }
            _ => {}
        }
    }

    None
}

fn parse_first_json_token(value: &str) -> Option<&str> {
    let bytes = value.as_bytes();
    let mut i = 0usize;
    while i < bytes.len() && bytes[i].is_ascii_whitespace() {
        i += 1;
    }
    if i >= bytes.len() {
        return None;
    }

    if bytes[i] == b']' {
        return None;
    }

    if bytes[i] == b'"' {
        let start = i;
        i += 1;
        let mut escaped = false;
        while i < bytes.len() {
            let b = bytes[i];
            if escaped {
                escaped = false;
            } else if b == b'\\' {
                escaped = true;
            } else if b == b'"' {
                return value.get(start..=i);
            }
            i += 1;
        }
        return None;
    }

    let start = i;
    while i < bytes.len() {
        let b = bytes[i];
        if b == b',' || b == b']' {
            return value.get(start..i).map(str::trim);
        }
        i += 1;
    }
    value.get(start..i).map(str::trim)
}

fn first_token_to_string(token: &str) -> Option<String> {
    let token = token.trim();
    if token.is_empty() || token == "null" {
        return None;
    }

    if token.starts_with('"') && token.ends_with('"') && token.len() >= 2 {
        let inner = &token[1..token.len() - 1];
        return Some(unescape_json_string(inner));
    }

    Some(token.to_string())
}

fn unescape_json_string(input: &str) -> String {
    let mut output = String::with_capacity(input.len());
    let mut chars = input.chars();
    while let Some(ch) = chars.next() {
        if ch != '\\' {
            output.push(ch);
            continue;
        }
        let Some(next) = chars.next() else {
            break;
        };
        match next {
            '"' => output.push('"'),
            '\\' => output.push('\\'),
            '/' => output.push('/'),
            'b' => output.push('\u{0008}'),
            'f' => output.push('\u{000C}'),
            'n' => output.push('\n'),
            'r' => output.push('\r'),
            't' => output.push('\t'),
            'u' => {
                let hex = chars.by_ref().take(4).collect::<String>();
                if hex.len() == 4 {
                    if let Ok(codepoint) = u16::from_str_radix(&hex, 16) {
                        if let Some(decoded) = char::from_u32(codepoint as u32) {
                            output.push(decoded);
                        }
                    }
                }
            }
            other => output.push(other),
        }
    }
    output
}

fn select_instances_for_viewer(
    instances: Vec<MetadataInstance>,
    requested_series_uid: Option<&str>,
) -> Result<Vec<MetadataInstance>> {
    if let Some(series_uid) = requested_series_uid {
        let filtered = instances
            .into_iter()
            .filter(|instance| instance.series_uid.as_deref() == Some(series_uid))
            .collect::<Vec<_>>();
        if filtered.is_empty() {
            bail!("Requested series UID was not found in study metadata");
        }
        return reduce_series_instances(filtered);
    }

    let grouped = group_by_series(instances);
    if grouped.is_empty() {
        bail!("Study metadata contains no series");
    }
    if grouped.len() == 1 {
        let (_, only_series) = grouped.into_iter().next().expect("len checked");
        return reduce_series_instances(only_series);
    }

    for series_instances in grouped.values() {
        if series_instances.len() == 4 {
            return reduce_series_instances(series_instances.clone());
        }
    }

    bail!(
        "Study contains multiple series. Add series UID in the perspecta URL for deterministic loading."
    )
}

fn group_by_series(instances: Vec<MetadataInstance>) -> BTreeMap<String, Vec<MetadataInstance>> {
    let mut grouped = BTreeMap::<String, Vec<MetadataInstance>>::new();
    for instance in instances {
        let key = instance
            .series_uid
            .clone()
            .unwrap_or_else(|| "UNKNOWN_SERIES".to_string());
        grouped.entry(key).or_default().push(instance);
    }
    grouped
}

fn reduce_series_instances(mut instances: Vec<MetadataInstance>) -> Result<Vec<MetadataInstance>> {
    if instances.len() == 1 {
        return Ok(instances);
    }

    sort_instances_for_mammo(&mut instances);
    if matches!(instances.len(), 2..=4) {
        return Ok(instances);
    }

    if instances.len() > 4 {
        if let Some(quartet) = pick_mammo_quartet(&instances) {
            return Ok(quartet);
        }
        if let Some(triplet) = pick_mammo_triplet(&instances) {
            return Ok(triplet);
        }
    }

    bail!(
        "Series has {} instances. Perspecta currently auto-opens 1 image, 2 images (1x2), 3 images (1x3), or a mammo quartet of 4.",
        instances.len()
    )
}

fn pick_mammo_quartet(instances: &[MetadataInstance]) -> Option<Vec<MetadataInstance>> {
    let mut rcc = None::<MetadataInstance>;
    let mut lcc = None::<MetadataInstance>;
    let mut rmlo = None::<MetadataInstance>;
    let mut lmlo = None::<MetadataInstance>;

    for instance in instances {
        let key = mammo_sort_key(instance);
        match key.0 {
            0 if key.1 == 0 && rcc.is_none() => rcc = Some(instance.clone()),
            0 if key.1 == 1 && lcc.is_none() => lcc = Some(instance.clone()),
            1 if key.1 == 0 && rmlo.is_none() => rmlo = Some(instance.clone()),
            1 if key.1 == 1 && lmlo.is_none() => lmlo = Some(instance.clone()),
            _ => {}
        }
    }

    Some(vec![rcc?, lcc?, rmlo?, lmlo?])
}

fn pick_mammo_triplet(instances: &[MetadataInstance]) -> Option<Vec<MetadataInstance>> {
    let mut rcc = None::<MetadataInstance>;
    let mut lcc = None::<MetadataInstance>;
    let mut rmlo = None::<MetadataInstance>;
    let mut lmlo = None::<MetadataInstance>;

    for instance in instances {
        let key = mammo_sort_key(instance);
        match key.0 {
            0 if key.1 == 0 && rcc.is_none() => rcc = Some(instance.clone()),
            0 if key.1 == 1 && lcc.is_none() => lcc = Some(instance.clone()),
            1 if key.1 == 0 && rmlo.is_none() => rmlo = Some(instance.clone()),
            1 if key.1 == 1 && lmlo.is_none() => lmlo = Some(instance.clone()),
            _ => {}
        }
    }

    let ordered = [rcc, lcc, rmlo, lmlo]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();
    if ordered.len() == 3 {
        Some(ordered)
    } else {
        None
    }
}

fn sort_instances_for_mammo(instances: &mut [MetadataInstance]) {
    instances.sort_by_key(mammo_sort_key);
}

fn mammo_sort_key(instance: &MetadataInstance) -> (u8, u8, i32, String) {
    let view_rank = match classify_view(instance.view_position.as_deref()) {
        Some("CC") => 0,
        Some("MLO") => 1,
        _ => 2,
    };
    let laterality_rank = match classify_laterality(instance.laterality.as_deref()) {
        Some("R") => 0,
        Some("L") => 1,
        _ => 2,
    };
    let instance_number = instance.instance_number.unwrap_or(i32::MAX);
    (
        view_rank,
        laterality_rank,
        instance_number,
        instance.instance_uid.clone(),
    )
}

fn download_instance(
    client: &Client,
    base: &str,
    request: DownloadInstanceRequest<'_>,
    auth: Option<(&str, &str)>,
) -> Result<DicomSource> {
    let DownloadInstanceRequest {
        study_uid,
        series_uid,
        sop_class_uid,
        modality,
        instance_uid,
    } = request;
    let mut urls = Vec::with_capacity(2);
    if let Some(series_uid) = series_uid {
        urls.push(format!(
            "{base}/studies/{study_uid}/series/{series_uid}/instances/{instance_uid}"
        ));
    }
    urls.push(format!(
        "{base}/studies/{study_uid}/instances/{instance_uid}"
    ));

    let accepts = preferred_accepts_for_instance(sop_class_uid);

    let mut last_error = None::<String>;
    let mut bytes = None::<Vec<u8>>;
    'attempts: for url in &urls {
        for accept in accepts {
            match http_get_bytes(client, url, accept, auth) {
                Ok(response_bytes) => {
                    let normalized = unwrap_dicom_multipart(response_bytes);
                    bytes = Some(normalized);
                    break 'attempts;
                }
                Err(err) => {
                    last_error = Some(format!("{url} (Accept: {accept}) => {err:#}"));
                }
            }
        }
    }

    let Some(bytes) = bytes else {
        let detail = last_error.unwrap_or_else(|| "no successful download attempts".to_string());
        bail!(
            "Failed downloading DICOM instance from study {study_uid}, series {:?}, instance {instance_uid}: {detail}",
            series_uid
        );
    };

    let identity_key = dicom_identity_key_from_parts(
        Some(study_uid),
        series_uid,
        Some(instance_uid),
        sop_class_uid,
        modality,
    );

    Ok(dicom_source_from_bytes_with_identity(
        instance_uid,
        identity_key,
        bytes,
    ))
}

fn preferred_accepts_for_instance(sop_class_uid: Option<&str>) -> &'static [&'static str] {
    if sop_class_uid.is_some_and(is_gsps_sop_class_uid) {
        &[
            "multipart/related; type=application/dicom",
            "multipart/related; type=\"application/dicom\"",
            "application/dicom",
            "application/dicom; transfer-syntax=*",
        ]
    } else {
        &[
            "application/dicom",
            "application/dicom; transfer-syntax=*",
            "multipart/related; type=application/dicom",
            "multipart/related; type=\"application/dicom\"",
        ]
    }
}

fn unwrap_dicom_multipart(body: Vec<u8>) -> Vec<u8> {
    match extract_dicom_from_multipart(&body) {
        Some(extracted) => extracted,
        None => body,
    }
}

fn download_instances_parallel(
    client: &Client,
    base: &str,
    study_uid: &str,
    auth: Option<(&str, &str)>,
    instances: &[MetadataInstance],
) -> Result<Vec<DicomSource>> {
    if instances.is_empty() {
        return Ok(Vec::new());
    }

    let mut outputs = (0..instances.len())
        .map(|_| None::<Result<DicomSource>>)
        .collect::<Vec<_>>();
    std::thread::scope(|scope| {
        let mut jobs = Vec::with_capacity(instances.len());
        for (index, instance) in instances.iter().enumerate() {
            jobs.push((
                index,
                scope.spawn(move || {
                    download_instance(
                        client,
                        base,
                        DownloadInstanceRequest {
                            study_uid,
                            series_uid: instance.series_uid.as_deref(),
                            sop_class_uid: instance.sop_class_uid.as_deref(),
                            modality: instance.modality.as_deref(),
                            instance_uid: &instance.instance_uid,
                        },
                        auth,
                    )
                }),
            ));
        }

        for (index, job) in jobs {
            outputs[index] = Some(
                job.join()
                    .unwrap_or_else(|_| bail!("DICOMweb download worker panicked")),
            );
        }
    });

    let mut paths = Vec::with_capacity(instances.len());
    for output in outputs {
        match output {
            Some(Ok(path)) => paths.push(path),
            Some(Err(err)) => return Err(err),
            None => bail!("DICOMweb download worker returned no result"),
        }
    }
    Ok(paths)
}

fn extract_dicom_from_multipart(body: &[u8]) -> Option<Vec<u8>> {
    let (line_end, line_sep_len) = find_line_end(body)?;
    let first_line = &body[..line_end];
    if !first_line.starts_with(b"--") || first_line.len() <= 2 {
        return None;
    }
    let boundary = &first_line[2..];

    let headers_start = line_end + line_sep_len;
    let (headers_end_rel, headers_sep_len) = find_headers_end(&body[headers_start..])?;
    let payload_start = headers_start + headers_end_rel + headers_sep_len;

    let payload_end = find_boundary_after_payload(body, payload_start, boundary)?;
    Some(body[payload_start..payload_end].to_vec())
}

fn find_line_end(bytes: &[u8]) -> Option<(usize, usize)> {
    if let Some(index) = find_subslice(bytes, b"\r\n") {
        return Some((index, 2));
    }
    find_subslice(bytes, b"\n").map(|index| (index, 1))
}

fn find_headers_end(bytes: &[u8]) -> Option<(usize, usize)> {
    if let Some(index) = find_subslice(bytes, b"\r\n\r\n") {
        return Some((index, 4));
    }
    find_subslice(bytes, b"\n\n").map(|index| (index, 2))
}

fn find_boundary_after_payload(
    body: &[u8],
    payload_start: usize,
    boundary: &[u8],
) -> Option<usize> {
    let mut marker = Vec::with_capacity(boundary.len() + 4);
    marker.extend_from_slice(b"\r\n--");
    marker.extend_from_slice(boundary);
    if let Some(index) = find_subslice(&body[payload_start..], &marker) {
        return Some(payload_start + index);
    }

    let mut marker_lf = Vec::with_capacity(boundary.len() + 3);
    marker_lf.extend_from_slice(b"\n--");
    marker_lf.extend_from_slice(boundary);
    find_subslice(&body[payload_start..], &marker_lf).map(|index| payload_start + index)
}

fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || needle.len() > haystack.len() {
        return None;
    }
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn http_get_text(
    client: &Client,
    url: &str,
    accept: &str,
    auth: Option<(&str, &str)>,
) -> Result<String> {
    let bytes = http_get_bytes(client, url, accept, auth)?;
    String::from_utf8(bytes).context("HTTP response was not valid UTF-8")
}

fn http_get_bytes(
    client: &Client,
    url: &str,
    accept: &str,
    auth: Option<(&str, &str)>,
) -> Result<Vec<u8>> {
    let mut request = client.get(url).header(ACCEPT, accept);
    if let Some((username, password)) = auth {
        request = request.basic_auth(username, Some(password));
    }

    let response = request
        .send()
        .with_context(|| format!("HTTP request failed for {url}"))?;
    let status = response.status();
    if !status.is_success() {
        let detail = response
            .text()
            .unwrap_or_else(|_| String::from("unable to read error body"));
        bail!("HTTP {status} for {url}: {detail}");
    }

    response
        .bytes()
        .map(|body| body.to_vec())
        .with_context(|| format!("Could not read response body from {url}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    use crate::dicom::{BASIC_TEXT_SR_SOP_CLASS_UID, GSPS_SOP_CLASS_UID};

    fn metadata_instance(
        instance_uid: &str,
        view_position: Option<&str>,
        laterality: Option<&str>,
        instance_number: Option<i32>,
    ) -> MetadataInstance {
        MetadataInstance {
            series_uid: Some("series_a".to_string()),
            instance_uid: instance_uid.to_string(),
            sop_class_uid: None,
            modality: Some("MG".to_string()),
            view_position: view_position.map(|value| value.to_string()),
            laterality: laterality.map(|value| value.to_string()),
            instance_number,
        }
    }

    #[test]
    fn split_top_level_objects_works() {
        let text = r#"[{"a":1},{"b":2},{"c":{"x":3}}]"#;
        let objects = split_top_level_json_objects(text).expect("should parse");
        assert_eq!(objects.len(), 3);
        assert!(objects[0].contains("\"a\":1"));
        assert!(objects[1].contains("\"b\":2"));
        assert!(objects[2].contains("\"c\""));
    }

    #[test]
    fn extract_first_tag_string_works() {
        let object = r#"{"00080018":{"vr":"UI","Value":["instance_uid_alpha"]},"00200013":{"vr":"IS","Value":[42]}}"#;
        assert_eq!(
            first_tag_string(object, TAG_SOP_INSTANCE_UID).as_deref(),
            Some("instance_uid_alpha")
        );
        assert_eq!(
            first_tag_string(object, TAG_INSTANCE_NUMBER).as_deref(),
            Some("42")
        );
    }

    #[test]
    fn extract_first_tag_string_ignores_nested_sequence_tags() {
        let object = r#"{
            "00081115":{"vr":"SQ","Value":[{"0020000E":{"vr":"UI","Value":["series_uid_nested"]}}]},
            "0020000E":{"vr":"UI","Value":["series_uid_top_level"]}
        }"#;
        assert_eq!(
            first_tag_string(object, TAG_SERIES_INSTANCE_UID).as_deref(),
            Some("series_uid_top_level")
        );
    }

    #[test]
    fn parse_metadata_instances_trims_sop_class_uid_and_modality() {
        let json = format!(
            r#"[{{"00080018":{{"vr":"UI","Value":["instance_uid_alpha"]}},"00080016":{{"vr":"UI","Value":["{} "]}},"00080060":{{"vr":"CS","Value":["MG "]}}, "0020000E":{{"vr":"UI","Value":["series_uid_alpha"]}}}}]"#,
            BASIC_TEXT_SR_SOP_CLASS_UID
        );

        let instances = parse_metadata_instances(&json).expect("metadata should parse");

        assert_eq!(instances.len(), 1);
        assert_eq!(
            instances[0].sop_class_uid.as_deref(),
            Some(BASIC_TEXT_SR_SOP_CLASS_UID)
        );
        assert_eq!(instances[0].modality.as_deref(), Some("MG"));
    }

    #[test]
    fn parse_metadata_instances_prefers_top_level_series_uid_for_gsps() {
        let json = format!(
            r#"[{{
                "00081115":{{"vr":"SQ","Value":[{{"0020000E":{{"vr":"UI","Value":["series_uid_referenced_image"]}}}}]}},
                "00080016":{{"vr":"UI","Value":["{}"]}},
                "00080018":{{"vr":"UI","Value":["instance_uid_gsps"]}},
                "00080060":{{"vr":"CS","Value":["PR"]}},
                "0020000E":{{"vr":"UI","Value":["series_uid_gsps_actual"]}}
            }}]"#,
            GSPS_SOP_CLASS_UID
        );

        let instances = parse_metadata_instances(&json).expect("metadata should parse");

        assert_eq!(instances.len(), 1);
        assert_eq!(
            instances[0].series_uid.as_deref(),
            Some("series_uid_gsps_actual")
        );
        assert_eq!(instances[0].instance_uid, "instance_uid_gsps");
        assert_eq!(instances[0].modality.as_deref(), Some("PR"));
        assert_eq!(
            instances[0].sop_class_uid.as_deref(),
            Some(GSPS_SOP_CLASS_UID)
        );
    }

    #[test]
    fn normalize_base_url_adds_dicomweb_path_for_root_url() {
        assert_eq!(
            normalize_base_url("http://localhost:8042"),
            "http://localhost:8042/dicom-web"
        );
        assert_eq!(
            normalize_base_url("http://localhost:8042/"),
            "http://localhost:8042/dicom-web"
        );
    }

    #[test]
    fn normalize_base_url_keeps_explicit_path() {
        assert_eq!(
            normalize_base_url("http://localhost:8042/dicom-web"),
            "http://localhost:8042/dicom-web"
        );
        assert_eq!(
            normalize_base_url("http://localhost:8042/server/dicom-web/"),
            "http://localhost:8042/server/dicom-web"
        );
    }

    #[test]
    fn metadata_url_uses_standard_wado_rs_paths() {
        assert_eq!(
            metadata_url("http://localhost:8042/dicom-web", "study_uid_alpha", None),
            "http://localhost:8042/dicom-web/studies/study_uid_alpha/metadata"
        );
        assert_eq!(
            metadata_url(
                "http://localhost:8042/dicom-web",
                "study_uid_alpha",
                Some("series_uid_beta"),
            ),
            "http://localhost:8042/dicom-web/studies/study_uid_alpha/series/series_uid_beta/metadata"
        );
    }

    #[test]
    fn extract_dicom_from_multipart_returns_payload() {
        let payload = b"DICOM-BYTES-\x00\x01\x02";
        let body = [
            b"--my-boundary\r\nContent-Type: application/dicom\r\n\r\n".as_slice(),
            payload.as_slice(),
            b"\r\n--my-boundary--\r\n".as_slice(),
        ]
        .concat();
        let extracted = extract_dicom_from_multipart(&body).expect("multipart should parse");
        assert_eq!(extracted, payload);
    }

    #[test]
    fn extract_dicom_from_multipart_ignores_plain_payload() {
        let body = b"plain-dicom-payload".to_vec();
        assert!(extract_dicom_from_multipart(&body).is_none());
    }

    #[test]
    fn reduce_series_instances_supports_three_up() {
        let reduced = reduce_series_instances(vec![
            metadata_instance("inst_lmlo", Some("MLO"), Some("L"), Some(3)),
            metadata_instance("inst_rcc", Some("CC"), Some("R"), Some(1)),
            metadata_instance("inst_rmlo", Some("MLO"), Some("R"), Some(2)),
        ])
        .expect("three-up series should be supported");

        let ordered_uids = reduced
            .into_iter()
            .map(|instance| instance.instance_uid)
            .collect::<Vec<_>>();
        assert_eq!(ordered_uids, vec!["inst_rcc", "inst_rmlo", "inst_lmlo"]);
    }

    #[test]
    fn reduce_series_instances_can_pick_triplet_from_larger_series() {
        let reduced = reduce_series_instances(vec![
            metadata_instance("inst_lmlo_1", Some("MLO"), Some("L"), Some(4)),
            metadata_instance("inst_rcc", Some("CC"), Some("R"), Some(1)),
            metadata_instance("inst_rmlo_1", Some("MLO"), Some("R"), Some(2)),
            metadata_instance("inst_rmlo_2", Some("MLO"), Some("R"), Some(3)),
            metadata_instance("inst_lmlo_2", Some("MLO"), Some("L"), Some(5)),
        ])
        .expect("larger series should reduce to a supported triplet");

        let ordered_uids = reduced
            .into_iter()
            .map(|instance| instance.instance_uid)
            .collect::<Vec<_>>();
        assert_eq!(ordered_uids, vec!["inst_rcc", "inst_rmlo_1", "inst_lmlo_1"]);
    }

    #[test]
    fn select_group_instances_single_reduced_set_keeps_all_in_order() {
        let selected = select_group_instances_from_reduced_sets(vec![vec![
            metadata_instance("inst_1", Some("CC"), Some("R"), Some(1)),
            metadata_instance("inst_2", Some("CC"), Some("L"), Some(2)),
            metadata_instance("inst_3", Some("MLO"), Some("R"), Some(3)),
        ]]);
        let uids = selected
            .into_iter()
            .map(|instance| instance.instance_uid)
            .collect::<Vec<_>>();
        assert_eq!(uids, vec!["inst_1", "inst_2", "inst_3"]);

        let empty_selected = select_group_instances_from_reduced_sets(vec![vec![]]);
        assert!(empty_selected.is_empty());
    }

    #[test]
    fn select_group_instances_two_four_image_series_returns_all_eight() {
        let selected = select_group_instances_from_reduced_sets(vec![
            vec![
                metadata_instance("curr_rcc", Some("CC"), Some("R"), Some(1)),
                metadata_instance("curr_lcc", Some("CC"), Some("L"), Some(2)),
                metadata_instance("curr_rmlo", Some("MLO"), Some("R"), Some(3)),
                metadata_instance("curr_lmlo", Some("MLO"), Some("L"), Some(4)),
            ],
            vec![
                metadata_instance("prior_rcc", Some("CC"), Some("R"), Some(1)),
                metadata_instance("prior_lcc", Some("CC"), Some("L"), Some(2)),
                metadata_instance("prior_rmlo", Some("MLO"), Some("R"), Some(3)),
                metadata_instance("prior_lmlo", Some("MLO"), Some("L"), Some(4)),
            ],
        ]);
        let uids = selected
            .into_iter()
            .map(|instance| instance.instance_uid)
            .collect::<Vec<_>>();
        assert_eq!(
            uids,
            vec![
                "curr_rcc",
                "curr_lcc",
                "curr_rmlo",
                "curr_lmlo",
                "prior_rcc",
                "prior_lcc",
                "prior_rmlo",
                "prior_lmlo",
            ]
        );
    }

    #[test]
    fn select_group_instances_fallback_picks_first_from_each_series() {
        let selected = select_group_instances_from_reduced_sets(vec![
            vec![],
            vec![
                metadata_instance("series_b_first", Some("CC"), Some("R"), Some(1)),
                metadata_instance("series_b_second", Some("MLO"), Some("R"), Some(2)),
            ],
            vec![metadata_instance(
                "series_c_first",
                Some("CC"),
                Some("L"),
                Some(1),
            )],
            vec![],
        ]);
        let uids = selected
            .into_iter()
            .map(|instance| instance.instance_uid)
            .collect::<Vec<_>>();
        assert_eq!(uids, vec!["series_b_first", "series_c_first"]);
    }

    #[test]
    fn displayable_group_image_count_excludes_structured_reports_and_gsps() {
        let image = metadata_instance("inst_image", Some("CC"), Some("R"), Some(1));
        let structured_report = MetadataInstance {
            instance_uid: "inst_sr".to_string(),
            sop_class_uid: Some(BASIC_TEXT_SR_SOP_CLASS_UID.to_string()),
            modality: Some("SR".to_string()),
            ..metadata_instance("inst_sr", None, None, Some(2))
        };
        let gsps = MetadataInstance {
            instance_uid: "inst_gsps".to_string(),
            sop_class_uid: Some(GSPS_SOP_CLASS_UID.to_string()),
            ..metadata_instance("inst_gsps", None, None, Some(3))
        };

        let instances = vec![image, structured_report, gsps];

        assert_eq!(displayable_group_image_count(&instances), 1);
        assert!(has_displayable_group_content(&instances));
    }

    #[test]
    fn has_displayable_group_content_accepts_structured_report_only_groups() {
        let instances = vec![MetadataInstance {
            instance_uid: "inst_sr".to_string(),
            sop_class_uid: Some(BASIC_TEXT_SR_SOP_CLASS_UID.to_string()),
            modality: Some("SR".to_string()),
            ..metadata_instance("inst_sr", None, None, Some(1))
        }];

        assert_eq!(displayable_group_image_count(&instances), 0);
        assert!(has_displayable_group_content(&instances));
    }

    #[test]
    fn active_group_instance_count_skips_structured_report_only_groups() {
        let sr_only = vec![MetadataInstance {
            instance_uid: "inst_sr".to_string(),
            sop_class_uid: Some(BASIC_TEXT_SR_SOP_CLASS_UID.to_string()),
            modality: Some("SR".to_string()),
            ..metadata_instance("inst_sr", None, None, Some(1))
        }];
        let mixed = vec![
            metadata_instance("inst_image", Some("CC"), Some("R"), Some(1)),
            MetadataInstance {
                instance_uid: "inst_sr".to_string(),
                sop_class_uid: Some(BASIC_TEXT_SR_SOP_CLASS_UID.to_string()),
                modality: Some("SR".to_string()),
                ..metadata_instance("inst_sr", None, None, Some(2))
            },
        ];

        assert_eq!(active_group_instance_count(&sr_only), None);
        assert_eq!(active_group_instance_count(&mixed), Some(1));
    }

    #[test]
    fn active_group_instance_count_ignores_supplementary_gsps() {
        let instances = vec![
            metadata_instance("inst_rcc", Some("CC"), Some("R"), Some(1)),
            metadata_instance("inst_lcc", Some("CC"), Some("L"), Some(2)),
            metadata_instance("inst_rmlo", Some("MLO"), Some("R"), Some(3)),
            metadata_instance("inst_lmlo", Some("MLO"), Some("L"), Some(4)),
            MetadataInstance {
                instance_uid: "inst_gsps".to_string(),
                sop_class_uid: Some(GSPS_SOP_CLASS_UID.to_string()),
                ..metadata_instance("inst_gsps", None, None, Some(5))
            },
        ];

        assert_eq!(displayable_group_image_count(&instances), 4);
        assert_eq!(active_group_instance_count(&instances), Some(4));
    }

    #[test]
    fn preferred_accepts_for_gsps_prioritize_multipart() {
        let accepts = preferred_accepts_for_instance(Some(GSPS_SOP_CLASS_UID));
        assert_eq!(accepts[0], "multipart/related; type=application/dicom");
        assert_eq!(accepts[1], "multipart/related; type=\"application/dicom\"");
    }

    #[test]
    fn preferred_accepts_for_images_keep_application_dicom_first() {
        let accepts = preferred_accepts_for_instance(None);
        assert_eq!(accepts[0], "application/dicom");
        assert_eq!(accepts[1], "application/dicom; transfer-syntax=*");
    }

    #[test]
    fn ordered_group_indices_prioritize_open_group() {
        assert_eq!(ordered_group_indices(4, 2), vec![2, 0, 1, 3]);
    }

    #[test]
    fn ordered_group_indices_clamp_out_of_range_open_group() {
        assert_eq!(ordered_group_indices(3, 99), vec![2, 0, 1]);
    }

    #[test]
    fn metadata_instance_kind_defaults_unknown_metadata_to_other() {
        let instance = MetadataInstance {
            series_uid: Some("series_a".to_string()),
            instance_uid: "inst_unknown".to_string(),
            sop_class_uid: None,
            modality: None,
            view_position: None,
            laterality: None,
            instance_number: Some(1),
        };

        assert_eq!(metadata_instance_kind(&instance), DicomPathKind::Other);
    }

    #[test]
    fn download_instances_streaming_emits_active_path_for_each_instance_in_order() {
        let instances = vec![
            MetadataInstance {
                series_uid: Some("series_a".to_string()),
                instance_uid: "inst_1".to_string(),
                sop_class_uid: None,
                modality: Some("MG".to_string()),
                view_position: Some("CC".to_string()),
                laterality: Some("R".to_string()),
                instance_number: Some(1),
            },
            MetadataInstance {
                series_uid: Some("series_a".to_string()),
                instance_uid: "inst_2".to_string(),
                sop_class_uid: None,
                modality: Some("MG".to_string()),
                view_position: Some("MLO".to_string()),
                laterality: Some("L".to_string()),
                instance_number: Some(2),
            },
        ];

        let mut updates = Vec::<DicomWebGroupStreamUpdate>::new();
        let mut on_path = |update: DicomWebGroupStreamUpdate| updates.push(update);
        let result = download_instances_streaming_with(&instances, &mut on_path, |instance| {
            Ok(DicomSource::from(PathBuf::from(format!(
                "{}.dcm",
                instance.instance_uid
            ))))
        })
        .expect("streaming should succeed");

        let callback_paths = updates
            .into_iter()
            .filter_map(|update| match update {
                DicomWebGroupStreamUpdate::ActivePath(path) => Some(path),
                DicomWebGroupStreamUpdate::ActiveGroupInstanceCount(_) => None,
            })
            .collect::<Vec<_>>();

        assert_eq!(
            callback_paths,
            vec![PathBuf::from("inst_1.dcm"), PathBuf::from("inst_2.dcm")]
        );
        assert_eq!(
            result,
            vec![PathBuf::from("inst_1.dcm"), PathBuf::from("inst_2.dcm")]
        );

        let mut memory_updates = Vec::<DicomWebGroupStreamUpdate>::new();
        let mut on_memory_path = |update: DicomWebGroupStreamUpdate| memory_updates.push(update);
        let memory_result =
            download_instances_streaming_with(&instances, &mut on_memory_path, |instance| {
                Ok(DicomSource::from_memory(
                    &instance.instance_uid,
                    instance.instance_uid.as_bytes().to_vec(),
                ))
            })
            .expect("memory-backed streaming should succeed");

        let memory_callback_paths = memory_updates
            .into_iter()
            .filter_map(|update| match update {
                DicomWebGroupStreamUpdate::ActivePath(path) => Some(path),
                DicomWebGroupStreamUpdate::ActiveGroupInstanceCount(_) => None,
            })
            .collect::<Vec<_>>();

        assert_eq!(
            memory_callback_paths
                .iter()
                .map(|path| path.short_label().into_owned())
                .collect::<Vec<_>>(),
            vec!["inst_1".to_string(), "inst_2".to_string()]
        );
        assert!(memory_callback_paths
            .iter()
            .all(|path| matches!(path, DicomSource::Memory { .. })));
        assert_eq!(
            memory_result
                .iter()
                .map(|path| path.short_label().into_owned())
                .collect::<Vec<_>>(),
            vec!["inst_1".to_string(), "inst_2".to_string()]
        );
        assert!(memory_result
            .iter()
            .all(|path| matches!(path, DicomSource::Memory { .. })));
    }
}
