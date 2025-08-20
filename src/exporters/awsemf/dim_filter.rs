use tower::BoxError;
use wildcard::{Wildcard, WildcardBuilder};

pub(crate) struct DimensionFilter {
    includes: Vec<Wildcard<'static, u8>>,
    excludes: Vec<Wildcard<'static, u8>>,
}

impl DimensionFilter {
    pub(crate) fn new(
        include_dimensions: Vec<String>,
        exclude_dimensions: Vec<String>,
    ) -> Result<Self, BoxError> {
        let includes = include_dimensions
            .into_iter()
            .map(|f| new_wildcard(f))
            .collect::<Result<Vec<_>, _>>()?;
        let excludes = exclude_dimensions
            .into_iter()
            .map(|f| new_wildcard(f))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { includes, excludes })
    }

    // Identify if the dimension should be included by checking the include
    // and exclude patterns. For complex filtering, we could possibly optimize
    // this by caching the result for common dimensions.
    pub(crate) fn should_include(&self, dimension_name: &str) -> bool {
        let name_bytes = dimension_name.as_bytes();

        // If there are include patterns, the dimension must match at least one
        if !self.includes.is_empty() {
            let matches_include = self
                .includes
                .iter()
                .any(|pattern| pattern.is_match(name_bytes));
            if !matches_include {
                return false;
            }
        }

        // If there are exclude patterns, the dimension must not match any
        if !self.excludes.is_empty() {
            let matches_exclude = self
                .excludes
                .iter()
                .any(|pattern| pattern.is_match(name_bytes));
            if matches_exclude {
                return false;
            }
        }

        true
    }
}

fn new_wildcard(s: String) -> Result<Wildcard<'static, u8>, BoxError> {
    // Leak the string to get 'static lifetime - this is intentional for filter patterns
    // that are expected to live for the duration of the program
    let leaked_bytes: &'static [u8] = Box::leak(s.into_bytes().into_boxed_slice());
    WildcardBuilder::new(leaked_bytes)
        .with_any_metasymbol('*' as u8) // Use * as the any symbol
        .without_one_metasymbol() // Disable ?
        .without_escape() // No escaping
        .build()
        .map_err(|e| e.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_no_filters_allows_all() {
        let filter = DimensionFilter::new(vec![], vec![]).unwrap();

        assert!(filter.should_include("service.name"));
        assert!(filter.should_include("http.method"));
    }

    #[test]
    fn test_include_exact_match() {
        let filter = DimensionFilter::new(
            vec!["service.name".to_string(), "http.method".to_string()],
            vec![],
        )
        .unwrap();

        assert!(filter.should_include("service.name"));
        assert!(filter.should_include("http.method"));
        assert!(!filter.should_include("other.dimension"));
        assert!(!filter.should_include("service.version"));
    }

    #[test]
    fn test_include_wildcard_patterns() {
        let filter =
            DimensionFilter::new(vec!["service.*".to_string(), "http.*".to_string()], vec![])
                .unwrap();

        assert!(filter.should_include("service.name"));
        assert!(filter.should_include("service.version"));
        assert!(filter.should_include("http.method"));
        assert!(filter.should_include("http.status_code"));
        assert!(!filter.should_include("other.dimension"));
        assert!(!filter.should_include("db.connection"));
    }

    #[test]
    fn test_exclude_exact_match() {
        let filter = DimensionFilter::new(
            vec![],
            vec!["service.name".to_string(), "http.method".to_string()],
        )
        .unwrap();

        assert!(!filter.should_include("service.name"));
        assert!(!filter.should_include("http.method"));
        assert!(filter.should_include("other.dimension"));
        assert!(filter.should_include("service.version"));
    }

    #[test]
    fn test_exclude_wildcard_patterns() {
        let filter =
            DimensionFilter::new(vec![], vec!["service.*".to_string(), "http.*".to_string()])
                .unwrap();

        assert!(!filter.should_include("service.name"));
        assert!(!filter.should_include("service.version"));
        assert!(!filter.should_include("http.method"));
        assert!(!filter.should_include("http.status_code"));
        assert!(filter.should_include("other.dimension"));
        assert!(filter.should_include("db.connection"));
    }

    #[test]
    fn test_include_and_exclude_combined() {
        let filter = DimensionFilter::new(
            vec!["service.*".to_string()],
            vec!["service.internal".to_string()],
        )
        .unwrap();

        assert!(filter.should_include("service.name"));
        assert!(filter.should_include("service.version"));
        assert!(!filter.should_include("service.internal")); // excluded
        assert!(!filter.should_include("http.method")); // not included
        assert!(!filter.should_include("other.dimension")); // not included
    }

    #[test]
    fn test_include_and_exclude_wildcard_combined() {
        let filter = DimensionFilter::new(
            vec!["*".to_string()],                                 // include all
            vec!["internal.*".to_string(), "debug.*".to_string()], // exclude internal and debug
        )
        .unwrap();

        assert!(filter.should_include("service.name"));
        assert!(filter.should_include("http.method"));
        assert!(!filter.should_include("internal.metric"));
        assert!(!filter.should_include("debug.info"));
        assert!(!filter.should_include("internal.counter"));
    }

    #[test]
    fn test_partial_wildcard_patterns() {
        let filter =
            DimensionFilter::new(vec!["*_total".to_string(), "cpu_*".to_string()], vec![]).unwrap();

        assert!(filter.should_include("requests_total"));
        assert!(filter.should_include("errors_total"));
        assert!(filter.should_include("cpu_usage"));
        assert!(filter.should_include("cpu_percent"));
        assert!(!filter.should_include("requests_count"));
        assert!(!filter.should_include("memory_usage"));
    }

    #[test]
    fn test_case_sensitive_matching() {
        let filter = DimensionFilter::new(vec!["Service.*".to_string()], vec![]).unwrap();

        assert!(filter.should_include("Service.Name"));
        assert!(!filter.should_include("service.name")); // lowercase doesn't match
        assert!(!filter.should_include("SERVICE.NAME")); // uppercase doesn't match
    }

    #[test]
    fn test_complex_exclude_priority() {
        // Include service.* but exclude service.internal.*
        let filter = DimensionFilter::new(
            vec!["service.*".to_string()],
            vec!["service.internal.*".to_string()],
        )
        .unwrap();

        assert!(filter.should_include("service.name"));
        assert!(filter.should_include("service.version"));
        assert!(!filter.should_include("service.internal.debug"));
        assert!(!filter.should_include("service.internal.metric"));
    }
}
