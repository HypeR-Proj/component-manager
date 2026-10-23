// SPDX-FileCopyrightText: 2026 roolrz
// SPDX-License-Identifier: Apache-2.0

//! Root-manager startup adapter. Bootstrap knows only its existing handoff ABI;
//! domain-specific resource adoption executes in the configured coordinator.

use super::*;
use hyper_bootstrap_handoff as handoff;
use hyper_os::{
    startup,
    task::{ProcessBuilder, StartFailure},
    wait::*,
};
use serde::Deserialize;
use std::io::Read;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Config {
    configuration_directory: String,
    root: String,
    startup: String,
}

/// Retained manager authority is limited to process construction and executable
/// lookup. Library directories do not carry INSPECT; TRANSFER is needed when
/// deriving executable capabilities for a child construction transaction.
fn authority_rights(key: u16) -> Rights {
    match key {
        3 => hyper_os::scheduling::RIGHTS
            .union(Rights::CREATE_PROCESS)
            .union(Rights::RETAIN_PROCESS)
            .union(Rights::CREATE_TASK_GROUP)
            .union(Rights::LOCK_MEMORY)
            .union(Rights::DUPLICATE),
        5 => Rights::RESOURCE_DOMAIN_SPONSOR.union(Rights::DUPLICATE),
        _ => Rights::READ
            .union(Rights::EXECUTE)
            .union(Rights::PROTECT)
            .union(Rights::DUPLICATE)
            .union(Rights::TRANSFER),
    }
}

fn validate_authority(offer: &handoff::Offer) -> Result<()> {
    for key in [1, 2, 3, 5, 33] {
        let slot = offer
            .slots
            .iter()
            .find(|s| s.key == key)
            .ok_or(Error::MissingHandle)?;
        if authority_rights(key).bits() & !slot.rights != 0 {
            return Err(Error::Status(hyper_os::Status::ACCESS_DENIED));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn launch_authority_is_checked_before_accepting_ownership() {
        let mut offer = handoff::Offer {
            version: 4,
            root_mount: 1,
            boot_root: "/boot".into(),
            services: Vec::new(),
            storage_successor: None,
            storage_ready: true,
            slots: [1, 2, 3, 5, 33]
                .into_iter()
                .map(|key| handoff::Slot {
                    key,
                    rights: authority_rights(key).bits(),
                })
                .collect(),
        };
        assert!(validate_authority(&offer).is_ok());
        assert!(!authority_rights(2).contains(Rights::INSPECT));
        offer.slots[2].rights &= !Rights::CREATE_TASK_GROUP.bits();
        assert!(validate_authority(&offer).is_err());
    }
}

fn configuration()
-> std::result::Result<(hyper_component_config::Topology, usize), Box<dyn std::error::Error>> {
    let mut bytes = Vec::new();
    std::fs::File::open("/etc/hyper/components.json")?
        .take(4097)
        .read_to_end(&mut bytes)?;
    if bytes.len() > 4096 {
        return Err("component manager configuration too large".into());
    }
    let config: Config = serde_json::from_slice(&bytes)?;
    if !hyper_component_config::absolute_path(&config.configuration_directory) {
        return Err("configuration directory must be absolute".into());
    }
    let mut documents = Vec::new();
    let mut total = 0;
    for entry in std::fs::read_dir(&config.configuration_directory)? {
        let entry = entry?;
        let filename = entry
            .file_name()
            .into_string()
            .map_err(|_| "invalid component filename")?;
        if !filename.ends_with(".json") {
            continue;
        }
        if documents.len() >= hyper_component_config::MAX_COMPONENTS {
            return Err("too many component declarations".into());
        }
        let mut bytes = Vec::new();
        std::fs::File::open(entry.path())?
            .take(hyper_component_config::MAX_DOCUMENT_BYTES as u64 + 1)
            .read_to_end(&mut bytes)?;
        total += bytes.len();
        if total > hyper_component_config::MAX_CONFIGURATION_BYTES {
            return Err("component configuration too large".into());
        }
        documents.push((filename, bytes));
    }
    let topology = hyper_component_config::Topology::parse(&config.root, documents)?;
    let coordinator = topology
        .index(&config.startup)
        .ok_or("startup instance is not declared")?;
    let program = topology.instances[coordinator]
        .declaration
        .program
        .as_ref()
        .ok_or("startup instance needs an executable")?;
    if !program.resident
        || program.prepared
        || program.restart != hyper_component_config::Restart::Never
    {
        return Err("startup coordinator must be resident and non-restarting".into());
    }
    Ok((topology, coordinator))
}

pub fn run() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let mut startup = hyper_rt::process::startup()?;
    if let Some(arguments) = startup.take_boot_command_line()?
        && !arguments.is_empty()
    {
        // The current formal environment defines no boot arguments. Report
        // leftovers once rather than injecting them into component argv/env.
        eprintln!("HypeR component-manager: unrecognized boot arguments: {arguments}");
    }
    let description = startup.take(handoff::DESCRIPTION)?;
    let offer = handoff::Offer::read(description.as_handle_ref())?;
    validate_authority(&offer)?;
    let (topology, coordinator) = configuration()?;
    handoff::lock_candidate(&startup, offer.root_mount)?;
    let root = startup.borrow(startup::ROOT_DIRECTORY)?;
    let root = hyper_os::fs::Directory::from_handle(root.duplicate(root.info()?.rights)?);
    let environment = hyper_service::process::open_environment(&root, "/")?;
    let libraries = hyper_service::process::open_library_directory(&environment)?;
    let program = topology.instances[coordinator]
        .declaration
        .program
        .as_ref()
        .ok_or("coordinator")?;
    let image = root.open(
        &program.image,
        hyper_service::process::EXECUTABLE_FILE_RIGHTS,
    )?;
    let loader = hyper_service::process::open_loader(
        &environment,
        hyper_service::process::LoaderKind::Dynamic,
    )?;
    let builder = ProcessBuilder::create(
        startup.borrow(startup::TASK_FACTORY)?,
        startup.borrow(startup::TASK_GROUP)?,
        startup.borrow(startup::RESOURCE_DOMAIN)?,
        image.as_handle_ref(),
        loader.as_handle_ref(),
    )?;
    builder.set_name("system-startup")?;
    super::scheduling::apply(&builder, program.scheduling)?;
    builder.add_argument(&program.image)?;
    for argument in &program.arguments {
        builder.add_argument(argument)?;
    }
    builder.set_memory_lock_allowed(true)?;
    builder.add_handle_duplicate(
        root.as_handle_ref(),
        startup::ROOT_DIRECTORY.as_raw(),
        RightsOffer::SameRights,
    )?;
    builder.add_handle_duplicate(
        libraries.as_handle_ref(),
        startup::DYNAMIC_LIBRARY_DIRECTORY.as_raw(),
        RightsOffer::Exact(Rights::READ.union(Rights::EXECUTE).union(Rights::PROTECT)),
    )?;
    builder.add_handle_duplicate(
        hyper_rt::process::console()?.as_handle_ref(),
        startup::CONSOLE.as_raw(),
        RightsOffer::SameRights,
    )?;
    builder
        .add_handle_move(
            description,
            handoff::DESCRIPTION.as_raw(),
            RightsOffer::Exact(Rights::READ.union(Rights::INSPECT)),
        )
        .map_err(|f| f.error())?;
    let (sender, receiver) = CapabilityChannel::create()?;
    builder
        .add_handle_move(
            receiver.into_handle(),
            handoff::ENDPOINT.as_raw(),
            RightsOffer::Exact(hyper_service_ipc::SERVER_RIGHTS),
        )
        .map_err(|f| f.error())?;
    // Root ingress must remain reversible until the coordinator has validated
    // its own resource requirements and registered the committed-inventory receive.
    // Management channels are created now; authority is filled after root commit.
    let lifecycle = Lifecycle::new(topology);
    // The preparation factory cannot create groups. It is used only for this
    // same-group coordinator; the full factory arrives in the atomic inventory.
    let (install_listener, install_connector) = CapabilityChannel::create()?;
    builder
        .add_handle_move(
            install_connector.into_handle(),
            installation::INSTALLER.as_raw(),
            RightsOffer::Exact(hyper_service_ipc::CLIENT_RIGHTS),
        )
        .map_err(|f| f.error())?;
    // Drain even before journal startup: error formatting alone can fill the
    // channel message quota. Temporary bounded relays preserve boot progress.
    let mut startup_logs = Vec::new();
    for (purpose, stream) in [
        (
            hyper_service::stdio::STANDARD_OUTPUT,
            hyper_journal::Stream::Stdout,
        ),
        (
            hyper_service::stdio::STANDARD_ERROR,
            hyper_journal::Stream::Stderr,
        ),
    ] {
        let (writer, reader) = hyper_os::channel::create_pair()?;
        let reader = super::startup_log::capture(reader)?;
        builder
            .add_handle_move(
                writer,
                purpose.as_raw(),
                RightsOffer::Exact(
                    Rights::WRITE
                        .union(Rights::WAIT)
                        .union(Rights::DUPLICATE)
                        .union(Rights::TRANSFER),
                ),
            )
            .map_err(|f| f.error())?;
        startup_logs.push((
            hyper_journal::Source {
                component: lifecycle.topology.instances[coordinator].path.clone(),
                generation: 1,
                stream,
            },
            reader,
        ));
    }
    builder.seal()?;
    let process = builder.start().map_err(|f| match f {
        StartFailure::Rejected { error, .. } | StartFailure::Committed(error) => error,
    })?;
    let until = hyper_os::time::deadline_after(std::time::Duration::from_secs(60))?.as_raw();
    let observation = wait_many(
        &[
            WaitItem::new(
                process.as_handle_ref(),
                ObjectSignals::<ProcessObject>::TERMINATED,
            ),
            WaitItem::new(
                sender.as_handle_ref(),
                ObjectSignals::<CapabilityChannelObject>::PEER_RECEIVING
                    .union(ObjectSignals::<CapabilityChannelObject>::PEER_CLOSED),
            ),
        ],
        until,
    )?;
    if observation.index != 1
        || !ObjectSignals::<CapabilityChannelObject>::PEER_RECEIVING
            .is_present_in(observation.observed)
    {
        super::display::startup_diagnostics(&mut startup_logs);
        return Err("startup preflight failed".into());
    }
    let endpoint = CapabilityChannel::from_handle(startup.take(handoff::ENDPOINT)?);
    let mut inventory = offer.receive(&endpoint, false, until)?;
    // Everything below is committed. Keep all original owners in this frame
    // until they are accepted by the coordinator or retained by the manager.
    let mut process_owner = Some(process);
    let mut manager_owner = None;
    let result = (|| -> std::result::Result<(), Box<dyn std::error::Error>> {
        inventory.commit_root()?;
        let directory = |key| -> Result<hyper_os::fs::Directory> {
            let value = inventory.get(key)?;
            let rights = authority_rights(key);
            let Object::Directory(Some(handle)) = value.duplicate(rights)? else {
                return Err(Error::InvalidResponse);
            };
            Ok(hyper_os::fs::Directory::from_handle(handle))
        };
        let Object::Factory(Some(factory)) = inventory.get(3)?.duplicate(authority_rights(3))?
        else {
            return Err("factory".into());
        };
        let Object::Domain(Some(domain)) = inventory.get(5)?.duplicate(authority_rights(5))? else {
            return Err("domain".into());
        };
        let authority = Authority {
            root: directory(1)?,
            libraries: directory(2)?,
            factory,
            domain,
            environment: directory(33)?,
        };
        manager_owner = Some(Manager::new(lifecycle, authority)?);
        let manager = manager_owner.as_mut().ok_or("manager unavailable")?;
        manager.startup_logs = startup_logs;
        manager.display =
            super::display::BootDisplay::begin(coordinator, &manager.lifecycle.topology).ok();
        manager.lifecycle.demand(coordinator)?;
        let starts = manager.lifecycle.begin_ready(now()?, |i| i == coordinator);
        if starts != [(coordinator, 1)] {
            return Err("invalid coordinator generation".into());
        }
        manager.lifecycle.started(
            coordinator,
            1,
            process_owner
                .as_ref()
                .ok_or("coordinator unavailable")?
                .basic_info()?
                .koid
                .get(),
        )?;
        manager.running[coordinator].process = process_owner.take();
        manager.lifecycle.consume_startup(coordinator);
        let (send, wake) = (manager.send.clone(), manager.wake.clone());
        std::thread::Builder::new()
            .name("component-install".into())
            .spawn(move || {
                loop {
                    match installation::receive(&install_listener) {
                        Ok(call) => emit(&send, &wake, Event::Install(call)),
                        Err(Error::Status(hyper_os::Status::PEER_CLOSED)) => return,
                        Err(_) => continue,
                    }
                }
            })?;
        // Never drop committed predecessors on a failed second rendezvous. Holding
        // this inventory keeps resources intact for a visible fatal diagnosis.
        inventory.forward(&offer, &sender, until)?;
        manager.run()?;
        Ok(())
    })();
    if let Err(error) = result {
        if let Some(manager) = &mut manager_owner
            && let Some(display) = &mut manager.display
        {
            display.finish(false);
        }
        eprintln!("HypeR component-manager: fatal after ownership commit: {error}");
        // No error path drops live supervision or claims a precommit rollback.
        loop {
            std::thread::park();
        }
    }
    Ok(())
}
