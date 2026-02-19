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
    if token.contains("MLO") {
        Some("MLO")
    } else if token.contains("CC") {
        Some("CC")
    } else {
        None
    }
}
