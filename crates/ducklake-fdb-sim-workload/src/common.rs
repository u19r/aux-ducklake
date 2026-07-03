use foundationdb_simulation::WorkloadContext;

pub(crate) const OPTION_ACTIVE_CLIENT_COUNT: &str = "activeClientCount";
pub(crate) const OPTION_PROFILE: &str = "profile";

pub(crate) fn option_or_default<T>(context: &WorkloadContext, name: &str, default: T) -> T
where
    T: std::str::FromStr,
{
    context.get_option(name).unwrap_or(default)
}

pub(crate) fn option_or_default_string(
    context: &WorkloadContext,
    name: &str,
    default: &str,
) -> String {
    context
        .get_option(name)
        .unwrap_or_else(|| default.to_owned())
}
