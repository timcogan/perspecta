use std::path::PathBuf;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DicomWebLaunchRequest {
    pub base_url: String,
    pub study_uid: String,
    pub series_uid: Option<String>,
    pub instance_uid: Option<String>,
    pub username: Option<String>,
    pub password: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DicomWebGroupedLaunchRequest {
    pub base_url: String,
    pub study_uid: String,
    pub groups: Vec<Vec<String>>,
    pub open_group: usize,
    pub username: Option<String>,
    pub password: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LaunchRequest {
    LocalPaths(Vec<PathBuf>),
    LocalGroups {
        groups: Vec<Vec<PathBuf>>,
        open_group: usize,
    },
    DicomWebGroups(DicomWebGroupedLaunchRequest),
    DicomWeb(DicomWebLaunchRequest),
}

pub fn parse_launch_request_from_args(args: &[String]) -> Result<Option<LaunchRequest>, String> {
    if args.is_empty() {
        return Ok(None);
    }

    if args.len() == 1 && is_perspecta_uri(&args[0]) {
        return parse_perspecta_uri(&args[0]).map(Some);
    }

    if args[0] == "--open" {
        if args.len() == 1 {
            return Err("Missing file path(s) after --open.".to_string());
        }
        return Ok(Some(LaunchRequest::LocalPaths(
            args[1..].iter().map(PathBuf::from).collect(),
        )));
    }

    Ok(Some(LaunchRequest::LocalPaths(
        args.iter().map(PathBuf::from).collect(),
    )))
}

pub fn parse_perspecta_uri(uri: &str) -> Result<LaunchRequest, String> {
    let rest = strip_perspecta_scheme(uri)
        .ok_or_else(|| "URL must start with perspecta://".to_string())?;

    let (location, query) = split_location_and_query(rest);
    let mut raw_paths = Vec::new();
    let mut grouped_paths = Vec::<Vec<String>>::new();
    let mut grouped_series_uids = Vec::<Vec<String>>::new();
    let mut dicomweb_base = None::<String>;
    let mut study_uid = None::<String>;
    let mut series_uid = None::<String>;
    let mut instance_uid = None::<String>;
    let mut dicomweb_username = None::<String>;
    let mut dicomweb_password = None::<String>;
    let mut open_group = None::<usize>;

    if let Some(path_from_location) = parse_location_path(location)? {
        raw_paths.push(path_from_location);
    }

    if let Some(query_string) = query {
        for pair in query_string.split('&') {
            if pair.is_empty() {
                continue;
            }
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            let key = key.trim().to_ascii_lowercase();
            let decoded_value = percent_decode(value)?;
            match key.as_str() {
                "path" | "file" => {
                    if !decoded_value.trim().is_empty() {
                        raw_paths.push(decoded_value);
                    }
                }
                "paths" | "files" => {
                    let split_paths = split_path_list(&decoded_value);
                    for p in split_paths {
                        if !p.trim().is_empty() {
                            raw_paths.push(p.to_string());
                        }
                    }
                }
                "group" => {
                    let group = split_path_list(&decoded_value)
                        .into_iter()
                        .filter(|path| !path.trim().is_empty())
                        .map(|path| path.to_string())
                        .collect::<Vec<_>>();
                    if !group.is_empty() {
                        grouped_paths.push(group);
                    }
                }
                "groups" => {
                    for group in decoded_value.split(';') {
                        let grouped = split_path_list(group)
                            .into_iter()
                            .filter(|path| !path.trim().is_empty())
                            .map(|path| path.to_string())
                            .collect::<Vec<_>>();
                        if !grouped.is_empty() {
                            grouped_paths.push(grouped);
                        }
                    }
                }
                "group_series" | "groupseries" | "series_group" => {
                    let group = split_path_list(&decoded_value)
                        .into_iter()
                        .filter(|series_uid| !series_uid.trim().is_empty())
                        .map(|series_uid| series_uid.trim().to_string())
                        .collect::<Vec<_>>();
                    if !group.is_empty() {
                        grouped_series_uids.push(group);
                    }
                }
                "open_group" | "opengroup" | "active_group" | "group_index" => {
                    if decoded_value.trim().is_empty() {
                        continue;
                    }
                    let parsed = decoded_value
                        .trim()
                        .parse::<usize>()
                        .map_err(|_| "open_group must be a non-negative integer.".to_string())?;
                    open_group = Some(parsed);
                }
                "dicomweb" | "dicomweb_url" | "base_url" | "wado_base" => {
                    let trimmed = decoded_value.trim();
                    if !trimmed.is_empty() {
                        let parsed = parse_dicomweb_value(trimmed);
                        if parsed.base_url.is_empty() {
                            return Err("DICOMweb URL must include a server base URL.".to_string());
                        }
                        dicomweb_base = Some(parsed.base_url);
                        if study_uid.is_none() {
                            study_uid = parsed.study_uid;
                        }
                        if series_uid.is_none() {
                            series_uid = parsed.series_uid;
                        }
                        if instance_uid.is_none() {
                            instance_uid = parsed.instance_uid;
                        }
                    }
                }
                "study" | "studyuid" | "studyinstanceuid" | "study_instance_uid" => {
                    if !decoded_value.trim().is_empty() {
                        study_uid = Some(decoded_value.trim().to_string());
                    }
                }
                "series" | "seriesuid" | "seriesinstanceuid" | "series_instance_uid" => {
                    if !decoded_value.trim().is_empty() {
                        series_uid = Some(decoded_value.trim().to_string());
                    }
                }
                "instance" | "instanceuid" | "sopinstanceuid" | "sop_instance_uid" => {
                    if !decoded_value.trim().is_empty() {
                        instance_uid = Some(decoded_value.trim().to_string());
                    }
                }
                "user" | "username" | "dicomweb_user" | "dicomweb_username" => {
                    if !decoded_value.trim().is_empty() {
                        dicomweb_username = Some(decoded_value.trim().to_string());
                    }
                }
                "pass" | "password" | "dicomweb_pass" | "dicomweb_password" => {
                    if !decoded_value.trim().is_empty() {
                        dicomweb_password = Some(decoded_value.trim().to_string());
                    }
                }
                "auth" | "dicomweb_auth" => {
                    let trimmed = decoded_value.trim();
                    if trimmed.is_empty() {
                        continue;
                    }
                    let Some((user, pass)) = trimmed.split_once(':') else {
                        return Err(
                            "auth must be encoded as username:password (percent-encoded)."
                                .to_string(),
                        );
                    };
                    if !user.trim().is_empty() {
                        dicomweb_username = Some(user.trim().to_string());
                    }
                    if !pass.trim().is_empty() {
                        dicomweb_password = Some(pass.trim().to_string());
                    }
                }
                _ => {}
            }
        }
    }

    if !grouped_paths.is_empty() {
        if !raw_paths.is_empty() {
            return Err(
                "Cannot mix grouped launch (group=...) with path=/paths= parameters.".to_string(),
            );
        }
        if dicomweb_base.is_some() {
            return Err("Cannot mix grouped local launch (group=...) with dicomweb=.".to_string());
        }
        if !grouped_series_uids.is_empty() {
            return Err(
                "Cannot mix grouped local launch (group=...) with grouped DICOMweb launch (group_series=...).".to_string(),
            );
        }

        let groups = grouped_paths
            .into_iter()
            .map(|group| group.into_iter().map(PathBuf::from).collect::<Vec<_>>())
            .collect::<Vec<_>>();

        for (index, group) in groups.iter().enumerate() {
            if group.len() != 1 && group.len() != 4 {
                return Err(format!(
                    "Group {} has {} paths. Each group must contain exactly 1 or 4 DICOM paths.",
                    index,
                    group.len()
                ));
            }
        }

        let open_group = open_group.unwrap_or(0).min(groups.len().saturating_sub(1));
        return Ok(LaunchRequest::LocalGroups { groups, open_group });
    }

    if !grouped_series_uids.is_empty() {
        if !raw_paths.is_empty() {
            return Err(
                "Cannot mix grouped DICOMweb launch (group_series=...) with path=/paths= parameters.".to_string(),
            );
        }
        if series_uid.is_some() || instance_uid.is_some() {
            return Err(
                "Cannot mix grouped DICOMweb launch (group_series=...) with series=/instance= parameters.".to_string(),
            );
        }
        if dicomweb_username.is_some() ^ dicomweb_password.is_some() {
            return Err("DICOMweb credentials must include both user and password.".to_string());
        }

        let Some(base_url) = dicomweb_base else {
            return Err(
                "Grouped DICOMweb launch requires dicomweb= URL and study UID.".to_string(),
            );
        };
        let Some(study_uid) = study_uid else {
            return Err("Grouped DICOMweb launch requires study UID via study=...".to_string());
        };

        for (index, group) in grouped_series_uids.iter().enumerate() {
            if group.len() != 1 && group.len() != 4 {
                return Err(format!(
                    "group_series group {} has {} series UIDs. Each group must contain exactly 1 or 4 series UIDs.",
                    index,
                    group.len()
                ));
            }
        }

        let open_group = open_group
            .unwrap_or(0)
            .min(grouped_series_uids.len().saturating_sub(1));
        return Ok(LaunchRequest::DicomWebGroups(
            DicomWebGroupedLaunchRequest {
                base_url,
                study_uid,
                groups: grouped_series_uids,
                open_group,
                username: dicomweb_username,
                password: dicomweb_password,
            },
        ));
    }

    if let Some(base_url) = dicomweb_base {
        if dicomweb_username.is_some() ^ dicomweb_password.is_some() {
            return Err("DICOMweb credentials must include both user and password.".to_string());
        }
        let Some(study_uid) = study_uid else {
            return Err("DICOMweb launch requires 'study' (StudyInstanceUID).".to_string());
        };
        return Ok(LaunchRequest::DicomWeb(DicomWebLaunchRequest {
            base_url,
            study_uid,
            series_uid,
            instance_uid,
            username: dicomweb_username,
            password: dicomweb_password,
        }));
    }

    if dicomweb_username.is_some() || dicomweb_password.is_some() {
        return Err("DICOMweb credentials were provided without dicomweb= URL.".to_string());
    }

    if raw_paths.is_empty() {
        return Err(
            "No DICOM path found in URL. Use path=..., file=..., paths=..., or files=..."
                .to_string(),
        );
    }

    Ok(LaunchRequest::LocalPaths(
        raw_paths.into_iter().map(PathBuf::from).collect(),
    ))
}

fn is_perspecta_uri(value: &str) -> bool {
    strip_perspecta_scheme(value).is_some()
}

fn strip_perspecta_scheme(uri: &str) -> Option<&str> {
    let prefix = "perspecta://";
    if uri.len() >= prefix.len() && uri[..prefix.len()].eq_ignore_ascii_case(prefix) {
        Some(&uri[prefix.len()..])
    } else {
        None
    }
}

fn split_location_and_query(value: &str) -> (&str, Option<&str>) {
    if let Some((location, query)) = value.split_once('?') {
        (location, Some(query))
    } else {
        (value, None)
    }
}

fn parse_location_path(location: &str) -> Result<Option<String>, String> {
    let location = location.trim();
    if location.is_empty() || location == "/" {
        return Ok(None);
    }

    let lower = location.to_ascii_lowercase();
    if lower == "open" {
        return Ok(None);
    }

    if lower.starts_with("open/") {
        let candidate = &location[5..];
        let decoded = percent_decode(candidate)?;
        if decoded.trim().is_empty() {
            return Ok(None);
        }
        return Ok(Some(decoded));
    }

    Ok(Some(percent_decode(location)?))
}

fn split_path_list(value: &str) -> Vec<&str> {
    if value.contains('|') {
        value.split('|').collect()
    } else {
        value.split(',').collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedDicomWebValue {
    base_url: String,
    study_uid: Option<String>,
    series_uid: Option<String>,
    instance_uid: Option<String>,
}

fn parse_dicomweb_value(value: &str) -> ParsedDicomWebValue {
    let without_query = strip_query_and_fragment(value.trim()).trim_end_matches('/');
    if without_query.is_empty() {
        return ParsedDicomWebValue {
            base_url: String::new(),
            study_uid: None,
            series_uid: None,
            instance_uid: None,
        };
    }

    let lower = without_query.to_ascii_lowercase();
    let marker = "/studies/";
    let Some(studies_index) = lower.find(marker) else {
        return ParsedDicomWebValue {
            base_url: without_query.to_string(),
            study_uid: None,
            series_uid: None,
            instance_uid: None,
        };
    };

    let base_url = without_query[..studies_index]
        .trim_end_matches('/')
        .to_string();
    let mut study_uid = None::<String>;
    let mut series_uid = None::<String>;
    let mut instance_uid = None::<String>;
    let remainder = &without_query[studies_index + 1..];
    let segments = remainder
        .split('/')
        .filter(|segment| !segment.trim().is_empty())
        .collect::<Vec<_>>();

    let mut i = 0usize;
    while i < segments.len() {
        match segments[i].to_ascii_lowercase().as_str() {
            "studies" => {
                if let Some(uid) = segments.get(i + 1) {
                    study_uid = Some((*uid).to_string());
                    i += 2;
                    continue;
                }
            }
            "series" => {
                if let Some(uid) = segments.get(i + 1) {
                    series_uid = Some((*uid).to_string());
                    i += 2;
                    continue;
                }
            }
            "instances" => {
                if let Some(uid) = segments.get(i + 1) {
                    instance_uid = Some((*uid).to_string());
                    i += 2;
                    continue;
                }
            }
            _ => {}
        }
        i += 1;
    }

    ParsedDicomWebValue {
        base_url,
        study_uid,
        series_uid,
        instance_uid,
    }
}

fn strip_query_and_fragment(value: &str) -> &str {
    let query_index = value.find('?').unwrap_or(value.len());
    let fragment_index = value.find('#').unwrap_or(value.len());
    &value[..query_index.min(fragment_index)]
}

fn percent_decode(value: &str) -> Result<String, String> {
    let bytes = value.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        match bytes[index] {
            b'+' => {
                decoded.push(b' ');
                index += 1;
            }
            b'%' => {
                if index + 2 >= bytes.len() {
                    return Err("Invalid percent-encoding in URL.".to_string());
                }
                let hi = decode_hex_digit(bytes[index + 1])
                    .ok_or_else(|| "Invalid percent-encoding in URL.".to_string())?;
                let lo = decode_hex_digit(bytes[index + 2])
                    .ok_or_else(|| "Invalid percent-encoding in URL.".to_string())?;
                decoded.push((hi << 4) | lo);
                index += 3;
            }
            byte => {
                decoded.push(byte);
                index += 1;
            }
        }
    }

    String::from_utf8(decoded).map_err(|_| "URL contains invalid UTF-8 after decoding.".to_string())
}

fn decode_hex_digit(value: u8) -> Option<u8> {
    match value {
        b'0'..=b'9' => Some(value - b'0'),
        b'a'..=b'f' => Some(value - b'a' + 10),
        b'A'..=b'F' => Some(value - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_single_path_query() {
        let request = parse_perspecta_uri("perspecta://open?path=example-data%2Fa.dcm")
            .expect("URI should parse");
        assert_eq!(
            request,
            LaunchRequest::LocalPaths(vec![PathBuf::from("example-data/a.dcm")])
        );
    }

    #[test]
    fn parse_repeated_path_params() {
        let request = parse_perspecta_uri(
            "perspecta://open?path=example-data%2Frcc.dcm&path=example-data%2Flcc.dcm&path=example-data%2Frmlo.dcm&path=example-data%2Flmlo.dcm",
        )
        .expect("URI should parse");
        assert_eq!(
            request,
            LaunchRequest::LocalPaths(vec![
                PathBuf::from("example-data/rcc.dcm"),
                PathBuf::from("example-data/lcc.dcm"),
                PathBuf::from("example-data/rmlo.dcm"),
                PathBuf::from("example-data/lmlo.dcm"),
            ])
        );
    }

    #[test]
    fn parse_dicomweb_request() {
        let request = parse_perspecta_uri(
            "perspecta://open?dicomweb=http%3A%2F%2Flocalhost%3A8042%2Fdicom-web&study=study_uid_alpha&series=series_uid_beta",
        )
        .expect("URI should parse");
        assert_eq!(
            request,
            LaunchRequest::DicomWeb(DicomWebLaunchRequest {
                base_url: "http://localhost:8042/dicom-web".to_string(),
                study_uid: "study_uid_alpha".to_string(),
                series_uid: Some("series_uid_beta".to_string()),
                instance_uid: None,
                username: None,
                password: None,
            })
        );
    }

    #[test]
    fn parse_dicomweb_root_with_auth() {
        let request = parse_perspecta_uri(
            "perspecta://open?dicomweb=http%3A%2F%2Flocalhost%3A8042&study_instance_uid=study_uid_alpha&user=vieweruser&password=viewerpass",
        )
        .expect("URI should parse");
        assert_eq!(
            request,
            LaunchRequest::DicomWeb(DicomWebLaunchRequest {
                base_url: "http://localhost:8042".to_string(),
                study_uid: "study_uid_alpha".to_string(),
                series_uid: None,
                instance_uid: None,
                username: Some("vieweruser".to_string()),
                password: Some("viewerpass".to_string()),
            })
        );
    }

    #[test]
    fn parse_dicomweb_embedded_path_extracts_uids() {
        let request = parse_perspecta_uri(
            "perspecta://open?dicomweb=http%3A%2F%2Flocalhost%3A8042%2Fdicom-web%2Fstudies%2Fstudy_uid_alpha%2Fseries%2Fseries_uid_beta%2Finstances%2Finstance_uid_gamma",
        )
        .expect("URI should parse");
        assert_eq!(
            request,
            LaunchRequest::DicomWeb(DicomWebLaunchRequest {
                base_url: "http://localhost:8042/dicom-web".to_string(),
                study_uid: "study_uid_alpha".to_string(),
                series_uid: Some("series_uid_beta".to_string()),
                instance_uid: Some("instance_uid_gamma".to_string()),
                username: None,
                password: None,
            })
        );
    }

    #[test]
    fn parse_dicomweb_requires_study() {
        let error = parse_perspecta_uri(
            "perspecta://open?dicomweb=http%3A%2F%2Flocalhost%3A8042%2Fdicom-web",
        )
        .expect_err("URI should fail");
        assert!(error.contains("requires 'study'"));
    }

    #[test]
    fn parse_dicomweb_auth_requires_user_and_password() {
        let error = parse_perspecta_uri(
            "perspecta://open?dicomweb=http%3A%2F%2Flocalhost%3A8042%2Fdicom-web&study=study_uid_alpha&user=vieweruser",
        )
        .expect_err("URI should fail");
        assert!(error.contains("both user and password"));
    }

    #[test]
    fn parse_dicomweb_grouped_series_request() {
        let request = parse_perspecta_uri(
            "perspecta://open?dicomweb=http%3A%2F%2Flocalhost%3A8042%2Fdicom-web&study=study_uid_alpha&group_series=series_a|series_b|series_c|series_d&group_series=series_report&open_group=0",
        )
        .expect("URI should parse");
        assert_eq!(
            request,
            LaunchRequest::DicomWebGroups(DicomWebGroupedLaunchRequest {
                base_url: "http://localhost:8042/dicom-web".to_string(),
                study_uid: "study_uid_alpha".to_string(),
                groups: vec![
                    vec![
                        "series_a".to_string(),
                        "series_b".to_string(),
                        "series_c".to_string(),
                        "series_d".to_string(),
                    ],
                    vec!["series_report".to_string()],
                ],
                open_group: 0,
                username: None,
                password: None,
            })
        );
    }

    #[test]
    fn parse_dicomweb_grouped_series_requires_dicomweb_url() {
        let error = parse_perspecta_uri(
            "perspecta://open?study=study_uid_alpha&group_series=series_a|series_b|series_c|series_d",
        )
        .expect_err("URI should fail");
        assert!(error.contains("Grouped DICOMweb launch requires dicomweb"));
    }

    #[test]
    fn parse_grouped_local_request() {
        let request = parse_perspecta_uri(
            "perspecta://open?group=example-data%2Frcc.dcm|example-data%2Flcc.dcm|example-data%2Frmlo.dcm|example-data%2Flmlo.dcm&group=example-data%2Freport.dcm",
        )
        .expect("URI should parse");
        assert_eq!(
            request,
            LaunchRequest::LocalGroups {
                groups: vec![
                    vec![
                        PathBuf::from("example-data/rcc.dcm"),
                        PathBuf::from("example-data/lcc.dcm"),
                        PathBuf::from("example-data/rmlo.dcm"),
                        PathBuf::from("example-data/lmlo.dcm"),
                    ],
                    vec![PathBuf::from("example-data/report.dcm")],
                ],
                open_group: 0,
            }
        );
    }

    #[test]
    fn parse_grouped_local_request_with_open_group() {
        let request = parse_perspecta_uri(
            "perspecta://open?group=example-data%2Fa.dcm&group=example-data%2Fb.dcm&open_group=1",
        )
        .expect("URI should parse");
        assert_eq!(
            request,
            LaunchRequest::LocalGroups {
                groups: vec![
                    vec![PathBuf::from("example-data/a.dcm")],
                    vec![PathBuf::from("example-data/b.dcm")],
                ],
                open_group: 1,
            }
        );
    }

    #[test]
    fn parse_cli_falls_back_to_raw_paths() {
        let args = vec![
            "example-data/a.dcm".to_string(),
            "example-data/b.dcm".to_string(),
            "example-data/c.dcm".to_string(),
            "example-data/d.dcm".to_string(),
        ];
        let parsed = parse_launch_request_from_args(&args).expect("args should parse");
        assert_eq!(
            parsed,
            Some(LaunchRequest::LocalPaths(vec![
                PathBuf::from("example-data/a.dcm"),
                PathBuf::from("example-data/b.dcm"),
                PathBuf::from("example-data/c.dcm"),
                PathBuf::from("example-data/d.dcm"),
            ]))
        );
    }
}
