//! Closed-world command-line registry for the service binary.

use std::io::Write;

use agent_first_data::{
    build_afdata_cli, cli_error_event, cli_help_event, cli_parse_output, cli_version_event,
    render_cli_reference, ArgSpec, BoundOutcome, BuiltCliSpec, CliEmitter, CliSpec, CliSpecError,
    Combination, CommandSpec, OutputFormat, OutputPlan, OutputSpec, OutputTo, ResolvedInvocation,
};

#[derive(Debug)]
pub struct Args {
    pub config: String,
    pub rebuild_search: bool,
}

fn service_output() -> OutputSpec {
    // The service's config owns its long-running log format; the CLI registry
    // owns stream redirection and otherwise preserves those bytes.
    OutputSpec::raw().file_sinks(["stdout", "stderr"])
}

fn lifecycle_output() -> OutputSpec {
    OutputSpec::protocol_finite(
        ["json", "yaml", "plain"],
        ["split", "stdout", "stderr"],
        "json",
        "split",
    )
    .file_sinks(["stdout", "stderr"])
}

fn cli_spec() -> Result<BuiltCliSpec, CliSpecError> {
    let mut spec = CliSpec::new("synapse", env!("CARGO_PKG_VERSION"))
        .display_name(env!("DISPLAY_NAME"))
        .about(env!("CARGO_PKG_DESCRIPTION"))
        .lifecycle_output(lifecycle_output())
        .command(
            CommandSpec::root()
                .about("Run the indexer or rebuild its search vectors")
                .arg(
                    ArgSpec::option("--config", "PATH")
                        .default("config.yml")
                        .about("Configuration file path"),
                )
                .arg(ArgSpec::flag("--rebuild-search").about(
                    "Rebuild the search vector index from storage, then exit; requires the \
                         ruvector feature and a running embedding server",
                ))
                .combination(
                    Combination::new("serve")
                        .action("serve")
                        .about("Run the federated indexer services")
                        .optional(["config"])
                        .output(service_output()),
                )
                .combination(
                    Combination::new("rebuild-search")
                        .action("rebuild_search")
                        .about("Re-embed every stored spore and exit")
                        .required(["rebuild_search"])
                        .optional(["config"])
                        .output(service_output()),
                ),
        );
    if let Some(build) = Some(env!("GIT_SHA")).filter(|sha| *sha != "unknown") {
        spec = spec.build_id(build);
    }
    build_afdata_cli(spec)
}

fn build_args(invocation: &ResolvedInvocation) -> Args {
    Args {
        config: invocation
            .optional("config")
            .and_then(agent_first_data::CliValue::as_str)
            .unwrap_or("config.yml")
            .to_string(),
        rebuild_search: invocation
            .optional("rebuild_search")
            .and_then(agent_first_data::CliValue::as_bool)
            .unwrap_or(false),
    }
}

/// Resolve argv before the service performs configuration, secret, or network I/O.
pub fn resolve_or_exit() -> (
    Args,
    Option<agent_first_data::stream_redirect::InstalledStreamRedirect>,
) {
    let cli = match cli_spec() {
        Ok(cli) => cli,
        Err(error) => emit_startup_error_or_exit("cli_spec_invalid", &error.to_string()),
    };
    let handler = build_args as fn(&ResolvedInvocation) -> Args;
    let app = match cli.bind_actions([("serve", handler), ("rebuild_search", handler)]) {
        Ok(app) => app,
        Err(error) => emit_startup_error_or_exit("cli_actions_invalid", &error.to_string()),
    };
    let outcome = match app.resolve_from(std::env::args_os()) {
        Ok(outcome) => outcome,
        Err(error) => emit_event_or_exit(
            cli_error_event(&error),
            OutputFormat::Json,
            OutputTo::Stderr,
            error.exit_code(),
        ),
    };

    match outcome {
        BoundOutcome::Run(invocation) => {
            let redirect = install_redirect_or_exit(invocation.output_plan());
            (invocation.run(), redirect)
        }
        BoundOutcome::Docs(docs) => {
            let _redirect = install_redirect_or_exit(docs.output_plan());
            write_text_or_exit(&render_cli_reference(&cli), OutputTo::Stdout)
        }
        BoundOutcome::Help(help) => {
            let _redirect = install_redirect_or_exit(help.output_plan());
            let format = format_of(help.output_plan());
            if format == OutputFormat::Plain {
                write_text_or_exit(&help.plain(), raw_destination(help.output_plan()));
            }
            emit_event_or_exit(
                cli_help_event(&help),
                format,
                destination_of(help.output_plan()),
                0,
            )
        }
        BoundOutcome::Version(version) => {
            let _redirect = install_redirect_or_exit(version.output_plan());
            emit_event_or_exit(
                cli_version_event(&version),
                format_of(version.output_plan()),
                destination_of(version.output_plan()),
                0,
            )
        }
    }
}

fn install_redirect_or_exit(
    plan: &OutputPlan,
) -> Option<agent_first_data::stream_redirect::InstalledStreamRedirect> {
    let config = match agent_first_data::stream_redirect::StreamRedirectConfig::new(
        plan.stdout_file().map(std::path::Path::to_path_buf),
        plan.stderr_file().map(std::path::Path::to_path_buf),
    ) {
        Ok(config) => config,
        Err(error) => emit_startup_error_or_exit("output_setup_failed", &error.to_string()),
    };
    match config
        .as_ref()
        .map(agent_first_data::stream_redirect::install)
        .transpose()
    {
        Ok(redirect) => redirect,
        Err(error) => emit_startup_error_or_exit("output_setup_failed", &error.to_string()),
    }
}

fn format_of(plan: &OutputPlan) -> OutputFormat {
    plan.format()
        .and_then(|format| cli_parse_output(format).ok())
        .unwrap_or(OutputFormat::Json)
}

fn destination_of(plan: &OutputPlan) -> OutputTo {
    plan.destination()
        .and_then(|destination| OutputTo::parse(destination).ok())
        .unwrap_or(OutputTo::Split)
}

fn raw_destination(plan: &OutputPlan) -> OutputTo {
    if plan.destination() == Some("stderr") {
        OutputTo::Stderr
    } else {
        OutputTo::Stdout
    }
}

fn emit_event_or_exit(
    event: agent_first_data::Event,
    format: OutputFormat,
    destination: OutputTo,
    exit_code: u8,
) -> ! {
    let mut emitter = CliEmitter::from_output_to(destination, format).with_strict_protocol();
    let code = match emitter.emit(event) {
        Ok(()) => exit_code,
        Err(_) => 4,
    };
    std::process::exit(i32::from(code))
}

fn emit_startup_error_or_exit(code: &str, message: &str) -> ! {
    let event = match agent_first_data::json_error(code, message).build() {
        Ok(event) => event,
        Err(_) => std::process::exit(4),
    };
    emit_event_or_exit(event, OutputFormat::Json, OutputTo::Stderr, 1)
}

/// Emit one strict terminal domain error after argv resolution has succeeded.
pub fn emit_domain_error(code: &str, message: &str, hint: &str) -> std::process::ExitCode {
    let event = match agent_first_data::json_error(code, message)
        .hint(hint)
        .build()
    {
        Ok(event) => event,
        Err(_) => return std::process::ExitCode::from(4),
    };
    let mut emitter =
        CliEmitter::from_output_to(OutputTo::Stderr, OutputFormat::Json).with_strict_protocol();
    std::process::ExitCode::from(emitter.finish(event, 1))
}

#[allow(clippy::disallowed_methods)]
fn write_text_or_exit(text: &str, destination: OutputTo) -> ! {
    let result = match destination {
        OutputTo::Stderr => std::io::stderr().lock().write_all(text.as_bytes()),
        OutputTo::Split | OutputTo::Stdout => std::io::stdout().lock().write_all(text.as_bytes()),
    };
    std::process::exit(if result.is_ok() { 0 } else { 4 })
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::panic)]
mod tests {
    use super::*;
    use agent_first_data::CliOutcome;

    #[test]
    fn every_registered_shape_resolves_and_dispatches() {
        let cli = cli_spec().unwrap();
        let handler = build_args as fn(&ResolvedInvocation) -> Args;
        let app = cli
            .bind_actions([("serve", handler), ("rebuild_search", handler)])
            .unwrap();
        // Both shapes dispatch, but only one asks for a rebuild: the registry
        // has to carry that distinction through to the args, not just resolve.
        let mut modes: Vec<(String, bool)> = app
            .call_every_combination()
            .into_iter()
            .map(|(combination, args)| (combination, args.rebuild_search))
            .collect();
        modes.sort();
        assert_eq!(
            modes,
            [
                ("rebuild-search".to_string(), true),
                ("serve".to_string(), false),
            ]
        );
    }

    #[test]
    fn help_exposes_both_legal_shapes_in_one_round_trip() {
        let cli = cli_spec().unwrap();
        let CliOutcome::Help(help) = cli.resolve_from(["synapse", "--help"]).unwrap() else {
            panic!("expected help");
        };
        assert_eq!(help.model().schema, "cli-help-v2");
        assert_eq!(help.model().shapes.len(), 2);
        assert!(help
            .model()
            .shapes
            .iter()
            .any(|shape| shape.usage.contains("--rebuild-search")));
        assert!(render_cli_reference(&cli).contains("rebuild-search"));
    }
}
