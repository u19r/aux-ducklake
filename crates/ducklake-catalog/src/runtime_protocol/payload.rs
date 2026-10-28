use std::sync::Arc;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RuntimePayload {
    Owned(Vec<u8>),
    Shared(Arc<Vec<u8>>),
}

impl From<Vec<u8>> for RuntimePayload {
    fn from(payload: Vec<u8>) -> Self {
        Self::Owned(payload)
    }
}

impl From<Arc<Vec<u8>>> for RuntimePayload {
    fn from(payload: Arc<Vec<u8>>) -> Self {
        Self::Shared(payload)
    }
}

impl AsRef<[u8]> for RuntimePayload {
    fn as_ref(&self) -> &[u8] {
        match self {
            Self::Owned(bytes) => bytes,
            Self::Shared(bytes) => bytes,
        }
    }
}

impl std::ops::Deref for RuntimePayload {
    type Target = [u8];
    fn deref(&self) -> &Self::Target {
        self.as_ref()
    }
}

impl RuntimePayload {
    pub(crate) fn into_vec(self) -> Vec<u8> {
        match self {
            Self::Owned(bytes) => bytes,
            Self::Shared(bytes) => Arc::unwrap_or_clone(bytes),
        }
    }
}
