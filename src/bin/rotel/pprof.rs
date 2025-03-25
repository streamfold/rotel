// SPDX-License-Identifier: Apache-2.0

use pprof::protos::Message;
use pprof::{ProfilerGuard, ProfilerGuardBuilder};
use std::fs::File;
use std::io::Write;

pub struct Guard<'a> {
    inner: ProfilerGuard<'a>,
}

pub fn pprof_guard<'a>() -> Option<Guard<'a>> {
    Some(Guard {
        inner: ProfilerGuardBuilder::default()
            .frequency(1000)
            .blocklist(&["libc", "libgcc", "pthread", "vdso"])
            .build()
            .unwrap(),
    })
}

pub fn pprof_finish(guard: Option<Guard>, with_flamegraph: bool, with_callgraph: bool) {
    if guard.is_none() {
        return;
    }

    let guard = guard.unwrap();
    if with_flamegraph {
        if let Ok(report) = guard.inner.report().build() {
            let file = File::create("flamegraph.svg").unwrap();
            let mut options = pprof::flamegraph::Options::default();
            options.image_width = Some(2500);
            report.flamegraph_with_options(file, &mut options).unwrap();
        }
    } else if with_callgraph {
        if let Ok(report) = guard.inner.report().build() {
            let mut file = File::create("profile.pb").unwrap();
            let profile = report.pprof().unwrap();
            let mut content = Vec::new();
            profile.encode(&mut content).unwrap();
            file.write_all(&content).unwrap();
        }
    }
}
