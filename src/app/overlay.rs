use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct OverlayNavigationTarget {
    viewport_index: usize,
    frame_index: usize,
}

impl DicomViewerApp {
    pub(super) fn merge_gsps_overlays(
        destination: &mut HashMap<String, GspsOverlay>,
        source: &HashMap<String, GspsOverlay>,
    ) {
        for (sop_uid, overlay) in source {
            if overlay.is_empty() {
                continue;
            }

            if let Some(existing_overlay) = destination.get_mut(sop_uid) {
                existing_overlay
                    .graphics
                    .extend(overlay.graphics.iter().cloned());
            } else {
                destination.insert(sop_uid.clone(), overlay.clone());
            }
        }
    }

    pub(super) fn merge_sr_overlays(
        destination: &mut HashMap<String, SrOverlay>,
        source: &HashMap<String, SrOverlay>,
    ) {
        for (sop_uid, overlay) in source {
            if overlay.is_empty() {
                continue;
            }

            if let Some(existing_overlay) = destination.get_mut(sop_uid) {
                existing_overlay
                    .graphics
                    .extend(overlay.graphics.iter().cloned());
            } else {
                destination.insert(sop_uid.clone(), overlay.clone());
            }
        }
    }

    pub(super) fn merge_pm_overlays(
        destination: &mut HashMap<String, ParametricMapOverlay>,
        source: &HashMap<String, ParametricMapOverlay>,
    ) {
        for (sop_uid, overlay) in source {
            if overlay.is_empty() {
                continue;
            }

            if let Some(existing_overlay) = destination.get_mut(sop_uid) {
                existing_overlay
                    .layers
                    .extend(overlay.layers.iter().cloned());
            } else {
                destination.insert(sop_uid.clone(), overlay.clone());
            }
        }
    }

    pub(super) fn insert_missing_gsps_overlays(
        destination: &mut HashMap<String, GspsOverlay>,
        source: HashMap<String, GspsOverlay>,
    ) -> bool {
        let mut inserted_any = false;

        for (sop_uid, overlay) in source {
            if overlay.is_empty() || destination.contains_key(&sop_uid) {
                continue;
            }

            destination.insert(sop_uid, overlay);
            inserted_any = true;
        }

        inserted_any
    }

    pub(super) fn insert_missing_sr_overlays(
        destination: &mut HashMap<String, SrOverlay>,
        source: HashMap<String, SrOverlay>,
    ) -> bool {
        let mut inserted_any = false;

        for (sop_uid, overlay) in source {
            if overlay.is_empty() || destination.contains_key(&sop_uid) {
                continue;
            }

            destination.insert(sop_uid, overlay);
            inserted_any = true;
        }

        inserted_any
    }

    pub(super) fn insert_missing_pm_overlays(
        destination: &mut HashMap<String, ParametricMapOverlay>,
        source: HashMap<String, ParametricMapOverlay>,
    ) -> bool {
        let mut inserted_any = false;

        for (sop_uid, overlay) in source {
            if overlay.is_empty() || destination.contains_key(&sop_uid) {
                continue;
            }

            destination.insert(sop_uid, overlay);
            inserted_any = true;
        }

        inserted_any
    }

    pub(super) fn attach_matching_gsps_overlay(
        image: &mut DicomImage,
        overlays: &HashMap<String, GspsOverlay>,
    ) {
        let matched_overlay = image
            .sop_instance_uid
            .as_ref()
            .and_then(|uid| overlays.get(uid))
            .cloned()
            .filter(|overlay| !overlay.is_empty());

        if let Some(overlay) = matched_overlay {
            image.gsps_overlay = Some(overlay);
        }
    }

    pub(super) fn attach_matching_sr_overlay(
        image: &mut DicomImage,
        overlays: &HashMap<String, SrOverlay>,
    ) {
        let matched_overlay = image
            .sop_instance_uid
            .as_ref()
            .and_then(|uid| overlays.get(uid))
            .cloned()
            .filter(|overlay| !overlay.is_empty());

        if let Some(overlay) = matched_overlay {
            image.sr_overlay = Some(overlay);
        }
    }

    pub(super) fn attach_matching_pm_overlay(
        image: &mut DicomImage,
        overlays: &HashMap<String, ParametricMapOverlay>,
    ) {
        let matched_overlay = image
            .sop_instance_uid
            .as_ref()
            .and_then(|uid| overlays.get(uid))
            .map(|overlay| {
                overlay.filtered_for_target(image.width, image.height, image.frame_count())
            });

        match matched_overlay {
            Some(overlay) if !overlay.is_empty() => {
                image.pm_overlay = Some(overlay);
            }
            Some(_) => {
                image.pm_overlay = None;
            }
            None => {}
        }
    }

    pub(super) fn attach_pending_overlays_to_current_study(&mut self) {
        if self.pending_gsps_overlays.is_empty()
            && self.pending_sr_overlays.is_empty()
            && self.pending_pm_overlays.is_empty()
        {
            return;
        }

        if let Some(image) = self.image.as_mut() {
            Self::attach_matching_gsps_overlay(image, &self.pending_gsps_overlays);
            Self::attach_matching_sr_overlay(image, &self.pending_sr_overlays);
            Self::attach_matching_pm_overlay(image, &self.pending_pm_overlays);
        }
        for viewport in self.mammo_group.iter_mut().filter_map(Option::as_mut) {
            Self::attach_matching_gsps_overlay(&mut viewport.image, &self.pending_gsps_overlays);
            Self::attach_matching_sr_overlay(&mut viewport.image, &self.pending_sr_overlays);
            Self::attach_matching_pm_overlay(&mut viewport.image, &self.pending_pm_overlays);
        }
    }

    pub(super) fn merge_pending_gsps_overlays(&mut self, overlays: HashMap<String, GspsOverlay>) {
        if overlays.is_empty() {
            return;
        }

        let mut merged_any = false;
        for (sop_uid, mut overlay) in overlays {
            if self.authoritative_gsps_overlay_keys.contains(&sop_uid) || overlay.is_empty() {
                continue;
            }
            self.pending_gsps_overlays
                .entry(sop_uid)
                .or_default()
                .graphics
                .append(&mut overlay.graphics);
            merged_any = true;
        }
        if !merged_any {
            return;
        }

        self.attach_pending_overlays_to_current_study();
        self.sync_current_state_to_history();
    }

    pub(super) fn merge_pending_sr_overlays(&mut self, overlays: HashMap<String, SrOverlay>) {
        if overlays.is_empty() {
            return;
        }

        let mut merged_any = false;
        for (sop_uid, mut overlay) in overlays {
            if self.authoritative_sr_overlay_keys.contains(&sop_uid) || overlay.is_empty() {
                continue;
            }
            self.pending_sr_overlays
                .entry(sop_uid)
                .or_default()
                .graphics
                .append(&mut overlay.graphics);
            merged_any = true;
        }
        if !merged_any {
            return;
        }

        self.attach_pending_overlays_to_current_study();
        self.sync_current_state_to_history();
    }

    pub(super) fn merge_pending_pm_overlays(
        &mut self,
        overlays: HashMap<String, ParametricMapOverlay>,
    ) {
        if overlays.is_empty() {
            return;
        }

        let mut merged_any = false;
        for (sop_uid, mut overlay) in overlays {
            if self.authoritative_pm_overlay_keys.contains(&sop_uid) || overlay.is_empty() {
                continue;
            }
            self.pending_pm_overlays
                .entry(sop_uid)
                .or_default()
                .layers
                .append(&mut overlay.layers);
            merged_any = true;
        }
        if !merged_any {
            return;
        }

        self.attach_pending_overlays_to_current_study();
        self.sync_current_state_to_history();
    }

    fn detach_removed_gsps_overlays_from_current_study(
        &mut self,
        removed_sop_uids: &HashSet<String>,
    ) {
        if removed_sop_uids.is_empty() {
            return;
        }

        if let Some(image) = self.image.as_mut() {
            if image
                .sop_instance_uid
                .as_ref()
                .is_some_and(|uid| removed_sop_uids.contains(uid))
            {
                image.gsps_overlay = None;
            }
        }
        for viewport in self.mammo_group.iter_mut().filter_map(Option::as_mut) {
            if viewport
                .image
                .sop_instance_uid
                .as_ref()
                .is_some_and(|uid| removed_sop_uids.contains(uid))
            {
                viewport.image.gsps_overlay = None;
            }
        }
    }

    fn detach_removed_sr_overlays_from_current_study(
        &mut self,
        removed_sop_uids: &HashSet<String>,
    ) {
        if removed_sop_uids.is_empty() {
            return;
        }

        if let Some(image) = self.image.as_mut() {
            if image
                .sop_instance_uid
                .as_ref()
                .is_some_and(|uid| removed_sop_uids.contains(uid))
            {
                image.sr_overlay = None;
            }
        }
        for viewport in self.mammo_group.iter_mut().filter_map(Option::as_mut) {
            if viewport
                .image
                .sop_instance_uid
                .as_ref()
                .is_some_and(|uid| removed_sop_uids.contains(uid))
            {
                viewport.image.sr_overlay = None;
            }
        }
    }

    fn detach_removed_sr_overlays_from_history(&mut self, removed_sop_uids: &HashSet<String>) {
        if removed_sop_uids.is_empty() {
            return;
        }

        for entry in &mut self.history_entries {
            match &mut entry.kind {
                HistoryKind::Single(single) => {
                    if single
                        .image
                        .sop_instance_uid
                        .as_ref()
                        .is_some_and(|uid| removed_sop_uids.contains(uid))
                    {
                        single.image.sr_overlay = None;
                    }
                }
                HistoryKind::Group(group) => {
                    for viewport in &mut group.viewports {
                        if viewport
                            .image
                            .sop_instance_uid
                            .as_ref()
                            .is_some_and(|uid| removed_sop_uids.contains(uid))
                        {
                            viewport.image.sr_overlay = None;
                        }
                    }
                }
                HistoryKind::Report(_) => {}
            }
        }
    }

    fn detach_removed_pm_overlays_from_current_study(
        &mut self,
        removed_sop_uids: &HashSet<String>,
    ) {
        if removed_sop_uids.is_empty() {
            return;
        }

        if let Some(image) = self.image.as_mut() {
            if image
                .sop_instance_uid
                .as_ref()
                .is_some_and(|uid| removed_sop_uids.contains(uid))
            {
                image.pm_overlay = None;
            }
        }
        for viewport in self.mammo_group.iter_mut().filter_map(Option::as_mut) {
            if viewport
                .image
                .sop_instance_uid
                .as_ref()
                .is_some_and(|uid| removed_sop_uids.contains(uid))
            {
                viewport.image.pm_overlay = None;
            }
        }
    }

    fn detach_removed_pm_overlays_from_history(&mut self, removed_sop_uids: &HashSet<String>) {
        if removed_sop_uids.is_empty() {
            return;
        }

        for entry in &mut self.history_entries {
            match &mut entry.kind {
                HistoryKind::Single(single) => {
                    if single
                        .image
                        .sop_instance_uid
                        .as_ref()
                        .is_some_and(|uid| removed_sop_uids.contains(uid))
                    {
                        single.image.pm_overlay = None;
                    }
                }
                HistoryKind::Group(group) => {
                    for viewport in &mut group.viewports {
                        if viewport
                            .image
                            .sop_instance_uid
                            .as_ref()
                            .is_some_and(|uid| removed_sop_uids.contains(uid))
                        {
                            viewport.image.pm_overlay = None;
                        }
                    }
                }
                HistoryKind::Report(_) => {}
            }
        }
    }

    fn detach_removed_gsps_overlays_from_history(&mut self, removed_sop_uids: &HashSet<String>) {
        if removed_sop_uids.is_empty() {
            return;
        }

        for entry in &mut self.history_entries {
            match &mut entry.kind {
                HistoryKind::Single(single) => {
                    if single
                        .image
                        .sop_instance_uid
                        .as_ref()
                        .is_some_and(|uid| removed_sop_uids.contains(uid))
                    {
                        single.image.gsps_overlay = None;
                    }
                }
                HistoryKind::Group(group) => {
                    for viewport in &mut group.viewports {
                        if viewport
                            .image
                            .sop_instance_uid
                            .as_ref()
                            .is_some_and(|uid| removed_sop_uids.contains(uid))
                        {
                            viewport.image.gsps_overlay = None;
                        }
                    }
                }
                HistoryKind::Report(_) => {}
            }
        }
    }

    pub(super) fn set_authoritative_pending_gsps_overlays(
        &mut self,
        overlays: HashMap<String, GspsOverlay>,
    ) {
        let overlays = overlays
            .into_iter()
            .filter(|(_, overlay)| !overlay.is_empty())
            .collect::<HashMap<_, _>>();
        let removed_sop_uids = self
            .pending_gsps_overlays
            .keys()
            .filter(|uid| !overlays.contains_key(*uid))
            .cloned()
            .collect::<HashSet<_>>();
        self.detach_removed_gsps_overlays_from_current_study(&removed_sop_uids);
        self.detach_removed_gsps_overlays_from_history(&removed_sop_uids);
        self.authoritative_gsps_overlay_keys = overlays.keys().cloned().collect();
        self.pending_gsps_overlays = overlays;
        self.attach_pending_overlays_to_current_study();
        self.sync_current_state_to_history();
    }

    pub(super) fn set_authoritative_pending_sr_overlays(
        &mut self,
        overlays: HashMap<String, SrOverlay>,
    ) {
        let overlays = overlays
            .into_iter()
            .filter(|(_, overlay)| !overlay.is_empty())
            .collect::<HashMap<_, _>>();
        let removed_sop_uids = self
            .pending_sr_overlays
            .keys()
            .filter(|uid| !overlays.contains_key(*uid))
            .cloned()
            .collect::<HashSet<_>>();
        self.detach_removed_sr_overlays_from_current_study(&removed_sop_uids);
        self.detach_removed_sr_overlays_from_history(&removed_sop_uids);
        self.authoritative_sr_overlay_keys = overlays.keys().cloned().collect();
        self.pending_sr_overlays = overlays;
        self.attach_pending_overlays_to_current_study();
        self.sync_current_state_to_history();
    }

    pub(super) fn set_authoritative_pending_pm_overlays(
        &mut self,
        overlays: HashMap<String, ParametricMapOverlay>,
    ) {
        let overlays = overlays
            .into_iter()
            .filter(|(_, overlay)| !overlay.is_empty())
            .collect::<HashMap<_, _>>();
        let removed_sop_uids = self
            .pending_pm_overlays
            .keys()
            .filter(|uid| !overlays.contains_key(*uid))
            .cloned()
            .collect::<HashSet<_>>();
        self.detach_removed_pm_overlays_from_current_study(&removed_sop_uids);
        self.detach_removed_pm_overlays_from_history(&removed_sop_uids);
        self.authoritative_pm_overlay_keys = overlays.keys().cloned().collect();
        self.pending_pm_overlays = overlays;
        self.attach_pending_overlays_to_current_study();
        self.sync_current_state_to_history();
    }

    pub(super) fn collect_grouped_gsps_overlays(
        prepared_groups: &[PreparedLoadPaths],
    ) -> HashMap<String, GspsOverlay> {
        let mut overlays = HashMap::new();
        for group in prepared_groups {
            Self::merge_gsps_overlays(&mut overlays, &group.gsps_overlays);
        }
        overlays
    }

    pub(super) fn collect_grouped_sr_overlays(
        prepared_groups: &[PreparedLoadPaths],
    ) -> HashMap<String, SrOverlay> {
        let mut overlays = HashMap::new();
        for group in prepared_groups {
            Self::merge_sr_overlays(&mut overlays, &group.sr_overlays);
        }
        overlays
    }

    pub(super) fn collect_grouped_pm_overlays(
        prepared_groups: &[PreparedLoadPaths],
    ) -> HashMap<String, ParametricMapOverlay> {
        let mut overlays = HashMap::new();
        for group in prepared_groups {
            Self::merge_pm_overlays(&mut overlays, &group.pm_overlays);
        }
        overlays
    }

    fn image_has_renderable_overlay(image: &DicomImage, frame_limit: usize) -> bool {
        !Self::overlay_target_frames(image, frame_limit).is_empty()
    }

    pub(super) fn has_available_overlay(&self) -> bool {
        if let Some(image) = self.image.as_ref() {
            return Self::image_has_renderable_overlay(image, image.frame_count());
        }

        let common_frame_count = self.mammo_group_common_frame_count();
        common_frame_count > 0
            && self.loaded_mammo_viewports().any(|viewport| {
                Self::image_has_renderable_overlay(&viewport.image, common_frame_count)
            })
    }

    pub(super) fn toggle_overlay(&mut self) -> bool {
        if !self.has_available_overlay() {
            self.overlay_visible = false;
            log::debug!("No overlay available for the current image or group.");
            return false;
        }
        self.overlay_visible = !self.overlay_visible;
        true
    }

    pub(super) fn refresh_active_textures(&mut self, ctx: &egui::Context) {
        if self.image.is_some() {
            self.rebuild_texture(ctx);
            ctx.request_repaint();
            return;
        }

        let mut missing_any = false;
        for viewport in self.mammo_group.iter_mut().filter_map(Option::as_mut) {
            let frame_count = viewport.image.frame_count();
            if frame_count == 0 {
                continue;
            }

            viewport.current_frame = viewport.current_frame.min(frame_count.saturating_sub(1));
            let Some(color_image) = Self::render_image_frame(
                &viewport.image,
                viewport.current_frame,
                viewport.window_center,
                viewport.window_width,
                self.overlay_visible,
            ) else {
                missing_any = true;
                continue;
            };
            viewport.texture.set(color_image, TextureOptions::LINEAR);
        }
        self.frame_wait_pending = missing_any;
        if missing_any {
            ctx.request_repaint_after(Duration::from_millis(16));
        } else {
            ctx.request_repaint();
        }
    }

    pub(super) fn overlay_target_frames(image: &DicomImage, frame_limit: usize) -> Vec<usize> {
        let frame_count = frame_limit.min(image.frame_count());
        if frame_count == 0 {
            return Vec::new();
        }

        let mut frame_targets = Vec::new();
        let mut applies_all_frames = false;

        if let Some(overlay) = image
            .gsps_overlay
            .as_ref()
            .filter(|overlay| !overlay.is_empty())
        {
            for graphic in &overlay.graphics {
                match graphic.referenced_frames.as_ref() {
                    None => applies_all_frames = true,
                    Some(referenced_frames) => {
                        for frame_number in referenced_frames {
                            let Some(stored_frame_index) = frame_number.checked_sub(1) else {
                                continue;
                            };
                            let Some(frame_index) =
                                image.stored_frame_index_to_display(stored_frame_index)
                            else {
                                continue;
                            };
                            if frame_index < frame_count {
                                frame_targets.push(frame_index);
                            }
                        }
                    }
                }
            }
        }

        if let Some(overlay) = image
            .sr_overlay
            .as_ref()
            .filter(|overlay| !overlay.is_empty())
        {
            for graphic in overlay
                .graphics
                .iter()
                .filter(|graphic| graphic.rendering_intent.is_visible_in_v1())
            {
                match graphic.referenced_frames.as_ref() {
                    None => applies_all_frames = true,
                    Some(referenced_frames) => {
                        for frame_number in referenced_frames {
                            let Some(stored_frame_index) = frame_number.checked_sub(1) else {
                                continue;
                            };
                            let Some(frame_index) =
                                image.stored_frame_index_to_display(stored_frame_index)
                            else {
                                continue;
                            };
                            if frame_index < frame_count {
                                frame_targets.push(frame_index);
                            }
                        }
                    }
                }
            }
        }

        if let Some(overlay) = image
            .pm_overlay
            .as_ref()
            .filter(|overlay| !overlay.is_empty())
        {
            for stored_frame_index in overlay.source_frame_indices(frame_count) {
                let Some(frame_index) = image.stored_frame_index_to_display(stored_frame_index)
                else {
                    continue;
                };
                if frame_index < frame_count {
                    frame_targets.push(frame_index);
                }
            }
        }

        if applies_all_frames {
            return (0..frame_count).collect();
        }

        frame_targets.sort_unstable();
        frame_targets.dedup();
        frame_targets
    }

    fn overlay_navigation_targets(&self) -> Vec<OverlayNavigationTarget> {
        if let Some(image) = self.image.as_ref() {
            return Self::overlay_target_frames(image, image.frame_count())
                .into_iter()
                .map(|frame_index| OverlayNavigationTarget {
                    viewport_index: 0,
                    frame_index,
                })
                .collect();
        }

        let common_frame_count = self.mammo_group_common_frame_count();
        let mut targets = Vec::new();
        for (viewport_index, viewport) in self.mammo_group.iter().enumerate() {
            let Some(viewport) = viewport.as_ref() else {
                continue;
            };

            for frame_index in Self::overlay_target_frames(&viewport.image, common_frame_count) {
                targets.push(OverlayNavigationTarget {
                    viewport_index,
                    frame_index,
                });
            }
        }

        targets
    }

    pub(super) fn next_overlay_navigation_target(&self) -> Option<OverlayNavigationTarget> {
        let targets = self.overlay_navigation_targets();
        if targets.is_empty() {
            return None;
        }

        let current_target = if self.image.is_some() {
            OverlayNavigationTarget {
                viewport_index: 0,
                frame_index: self.current_frame,
            }
        } else {
            let current_frame = self
                .mammo_group
                .get(self.mammo_selected_index)
                .and_then(Option::as_ref)
                .map(|viewport| viewport.current_frame)
                .unwrap_or(0);
            OverlayNavigationTarget {
                viewport_index: self.mammo_selected_index,
                frame_index: current_frame,
            }
        };

        let target_index = match targets.iter().position(|target| *target == current_target) {
            Some(index) => (index + 1) % targets.len(),
            None => targets
                .iter()
                .position(|target| *target > current_target)
                .unwrap_or(0),
        };
        targets.get(target_index).copied()
    }

    pub(super) fn jump_to_next_overlay(&mut self, ctx: &egui::Context) {
        let Some(target) = self.next_overlay_navigation_target() else {
            log::debug!("No overlay target available for the current image or group.");
            return;
        };

        let overlay_was_hidden = !self.overlay_visible;
        self.overlay_visible = true;
        self.last_cine_advance = Some(Instant::now());

        if self.image.is_some() {
            self.current_frame = target.frame_index;
            self.rebuild_texture(ctx);
            ctx.request_repaint();
            return;
        }

        self.mammo_selected_index = target.viewport_index;
        if self.set_mammo_group_frame(target.frame_index) {
            if overlay_was_hidden {
                self.refresh_active_textures(ctx);
            } else {
                ctx.request_repaint_after(Duration::from_millis(16));
            }
        } else if overlay_was_hidden {
            self.refresh_active_textures(ctx);
        } else {
            ctx.request_repaint();
        }
    }
}
