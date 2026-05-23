pub struct ApiVersion {
    pub api_key: i16,
    pub min_api_version: i16,
    pub max_api_version: i16,
}

pub const API_VERSIONS: &[ApiVersion] = &[
    ApiVersion { api_key:  1, min_api_version: 0, max_api_version: 17 },
    ApiVersion { api_key: 18, min_api_version: 0, max_api_version: 4 },
    ApiVersion { api_key: 75, min_api_version: 0, max_api_version: 0 },
];

pub const VALID_API_VERSIONS: &[i16] = &[0, 1, 2, 3, 4];
