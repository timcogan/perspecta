use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use reqwest::blocking::Client;
use reqwest::header::ACCEPT;

use crate::launch::{DicomWebGroupedLaunchRequest, DicomWebLaunchRequest};

const TAG_SOP_INSTANCE_UID: &str = "00080018";
const TAG_SERIES_INSTANCE_UID: &str = "0020000E";
const TAG_INSTANCE_NUMBER: &str = "00200013";
const TAG_VIEW_POSITION: &str = "00185101";
const TAG_IMAGE_LATERALITY: &str = "00200062";
const TAG_LATERALITY: &str = "00200060";

#[derive(Clone)]
struct MetadataInstance {
    series_uid: Option<String>,
    instance_uid: String,
    view_position: Option<String>,
    laterality: Option<String>,
    instance_number: Option<i32>,
}

#[derive(Debug, Clone)]
pub enum DicomWebDownloadResult {
    Single(Vec<PathBuf>),
    Grouped {
        groups: Vec<Vec<PathBuf>>,
        open_group: usize,
    },
}

pub fn download_dicomweb_request(
    request: &DicomWebLaunchRequest,
) -> Result<DicomWebDownloadResult> {
    let client = build_http_client()?;
    let base = normalize_base_url(&request.base_url);
    let auth = request.username.as_deref().zip(request.password.as_deref());
    let cache_dir = create_cache_dir()?;

    if let Some(instance_uid) = request.instance_uid.as_ref() {
        let path = download_instance(
            &client,
            &base,
            &request.study_uid,
            request.series_uid.as_deref(),
            instance_uid,
            &cache_dir,
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
    let paths = download_instances_parallel(
        &client,
        &base,
        &request.study_uid,
        &cache_dir,
        auth,
        &selected,
    )?;

    Ok(DicomWebDownloadResult::Single(paths))
}

pub fn download_dicomweb_group_request<F>(
    request: &DicomWebGroupedLaunchRequest,
    mut on_active_path: F,
) -> Result<DicomWebDownloadResult>
where
    F: FnMut(PathBuf),
{
    let client = build_http_client()?;
    let base = normalize_base_url(&request.base_url);
    let auth = request.username.as_deref().zip(request.password.as_deref());
    let cache_dir = create_cache_dir()?;

    if request.groups.is_empty() {
        bail!("DICOMweb grouped launch requested no groups");
    }

    let mut downloaded_groups = Vec::with_capacity(request.groups.len());

    for (group_index, group_series_uids) in request.groups.iter().enumerate() {
        if group_series_uids.len() != 1 && group_series_uids.len() != 4 {
            bail!(
                "DICOMweb group {} has {} series UIDs; each group must contain exactly 1 or 4 series UIDs",
                group_index,
                group_series_uids.len()
            );
        }

        let mut selected_instances = Vec::<MetadataInstance>::new();

        for series_uid in group_series_uids {
            let metadata_instances = fetch_instance_metadata(
                &client,
                &base,
                &request.study_uid,
                Some(series_uid.as_str()),
                auth,
            )
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

            let mut reduced = reduce_series_instances(metadata_instances).with_context(|| {
                format!(
                    "Group {} series {} did not resolve to a supported instance set",
                    group_index, series_uid
                )
            })?;

            if group_series_uids.len() == 1 {
                selected_instances.append(&mut reduced);
            } else if let Some(first) = reduced.into_iter().next() {
                selected_instances.push(first);
            }
        }

        if selected_instances.len() != 1 && selected_instances.len() != 4 {
            bail!(
                "DICOMweb group {} resolved to {} instances; each group must resolve to 1 or 4 DICOM instances",
                group_index,
                selected_instances.len()
            );
        }

        let group_paths = if group_index == request.open_group {
            download_instances_streaming(
                &client,
                &base,
                &request.study_uid,
                &cache_dir,
                auth,
                &selected_instances,
                &mut on_active_path,
            )?
        } else {
            download_instances_parallel(
                &client,
                &base,
                &request.study_uid,
                &cache_dir,
                auth,
                &selected_instances,
            )?
        };

        downloaded_groups.push(group_paths);
    }

    let open_group = request
        .open_group
        .min(downloaded_groups.len().saturating_sub(1));

    Ok(DicomWebDownloadResult::Grouped {
        groups: downloaded_groups,
        open_group,
    })
}

fn download_instances_streaming<F>(
    client: &Client,
    base: &str,
    study_uid: &str,
    cache_dir: &Path,
    auth: Option<(&str, &str)>,
    instances: &[MetadataInstance],
    on_path: &mut F,
) -> Result<Vec<PathBuf>>
where
    F: FnMut(PathBuf),
{
    let mut paths = Vec::with_capacity(instances.len());
    for instance in instances {
        let path = download_instance(
            client,
            base,
            study_uid,
            instance.series_uid.as_deref(),
            &instance.instance_uid,
            cache_dir,
            auth,
        )?;
        on_path(path.clone());
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

fn create_cache_dir() -> Result<PathBuf> {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    let dir = std::env::temp_dir().join(format!("perspecta-dicomweb-{}-{ts}", std::process::id()));
    fs::create_dir_all(&dir)
        .with_context(|| format!("Could not create temporary directory {}", dir.display()))?;
    Ok(dir)
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
    let needle = format!("\"{tag}\"");
    let tag_pos = object.find(&needle)?;
    let tail = &object[tag_pos + needle.len()..];
    let value_pos = tail.find("\"Value\"")?;
    let after_value = &tail[value_pos + "\"Value\"".len()..];
    let array_start = after_value.find('[')?;
    let after_array_start = &after_value[array_start + 1..];
    let first_token = parse_first_json_token(after_array_start)?;
    first_token_to_string(first_token)
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
    if instances.len() == 4 {
        return Ok(instances);
    }

    if instances.len() > 4 {
        if let Some(quartet) = pick_mammo_quartet(&instances) {
            return Ok(quartet);
        }
    }

    bail!(
        "Series has {} instances. Perspecta currently auto-opens 1 image or a mammo quartet of 4.",
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

fn classify_laterality(value: Option<&str>) -> Option<&'static str> {
    let token = normalize_token(value);
    if token.starts_with('R') || token.contains("RIGHT") {
        Some("R")
    } else if token.starts_with('L') || token.contains("LEFT") {
        Some("L")
    } else {
        None
    }
}

fn classify_view(value: Option<&str>) -> Option<&'static str> {
    let token = normalize_token(value);
    if token.contains("MLO") {
        Some("MLO")
    } else if token.contains("CC") {
        Some("CC")
    } else {
        None
    }
}

fn normalize_token(value: Option<&str>) -> String {
    value
        .unwrap_or_default()
        .trim()
        .to_ascii_uppercase()
        .replace(' ', "")
}

fn download_instance(
    client: &Client,
    base: &str,
    study_uid: &str,
    series_uid: Option<&str>,
    instance_uid: &str,
    output_dir: &Path,
    auth: Option<(&str, &str)>,
) -> Result<PathBuf> {
    let mut urls = Vec::with_capacity(2);
    if let Some(series_uid) = series_uid {
        urls.push(format!(
            "{base}/studies/{study_uid}/series/{series_uid}/instances/{instance_uid}"
        ));
    }
    urls.push(format!(
        "{base}/studies/{study_uid}/instances/{instance_uid}"
    ));

    let accepts = [
        "application/dicom",
        "application/dicom; transfer-syntax=*",
        "multipart/related; type=application/dicom",
        "multipart/related; type=\"application/dicom\"",
    ];

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

    let file_name = sanitize_for_file_name(instance_uid);
    let path = output_dir.join(format!("{file_name}.dcm"));
    fs::write(&path, &bytes)
        .with_context(|| format!("Could not write downloaded DICOM file {}", path.display()))?;
    Ok(path)
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
    cache_dir: &Path,
    auth: Option<(&str, &str)>,
    instances: &[MetadataInstance],
) -> Result<Vec<PathBuf>> {
    if instances.is_empty() {
        return Ok(Vec::new());
    }

    let mut outputs = (0..instances.len())
        .map(|_| None::<Result<PathBuf>>)
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
                        study_uid,
                        instance.series_uid.as_deref(),
                        &instance.instance_uid,
                        cache_dir,
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

fn sanitize_for_file_name(value: &str) -> String {
    value
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '.' || ch == '-' || ch == '_' {
                ch
            } else {
                '_'
            }
        })
        .collect::<String>()
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
