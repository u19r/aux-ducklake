use crate::{DataFileId, DeleteFileRow};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum DeleteFileRole {
    HistoricalDeleteFile,
    MaterializeInlineDeletesForDataFile,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DeleteFileMaterialization {
    row: DeleteFileRow,
    role: DeleteFileRole,
}

impl DeleteFileMaterialization {
    pub(crate) fn historical_delete_file(row: DeleteFileRow) -> Self {
        Self {
            row,
            role: DeleteFileRole::HistoricalDeleteFile,
        }
    }

    pub(crate) fn rows(materializations: &[Self]) -> Vec<DeleteFileRow> {
        materializations
            .iter()
            .map(|materialization| materialization.row.clone())
            .collect()
    }

    pub(crate) fn row(&self) -> &DeleteFileRow {
        &self.row
    }

    pub(crate) fn row_mut(&mut self) -> &mut DeleteFileRow {
        &mut self.row
    }

    pub(crate) fn mark_materializes_inline_deletes(&mut self) {
        self.role = DeleteFileRole::MaterializeInlineDeletesForDataFile;
    }

    pub(crate) fn data_file_id(&self) -> DataFileId {
        self.row.data_file_id
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn materializes_inline_deletes(&self) -> bool {
        self.role == DeleteFileRole::MaterializeInlineDeletesForDataFile
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn is_historical_delete_file(&self) -> bool {
        self.role == DeleteFileRole::HistoricalDeleteFile
    }
}
