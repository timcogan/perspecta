#![allow(dead_code)]

use anyhow::{bail, Result};

use crate::dicom::DicomSource;
use crate::launch::{DicomWebGroupedLaunchRequest, DicomWebLaunchRequest};

pub enum DicomWebDownloadResult {
    Single(Vec<DicomSource>),
    Grouped {
        groups: Vec<Vec<DicomSource>>,
        open_group: usize,
    },
}

pub enum DicomWebGroupStreamUpdate {
    ActiveGroupInstanceCount(usize),
    ActivePath(DicomSource),
    BackgroundGroupReady {
        group_index: usize,
        paths: Vec<DicomSource>,
    },
}

pub fn download_dicomweb_request(
    _request: &DicomWebLaunchRequest,
) -> Result<DicomWebDownloadResult> {
    bail!("DICOMweb is unavailable in the browser preview")
}

pub fn download_dicomweb_group_request<F>(
    _request: &DicomWebGroupedLaunchRequest,
    _on_active_path: F,
) -> Result<DicomWebDownloadResult>
where
    F: FnMut(DicomWebGroupStreamUpdate),
{
    bail!("DICOMweb is unavailable in the browser preview")
}
