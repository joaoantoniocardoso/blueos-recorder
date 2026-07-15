pub mod vehicle;

use ::mavlink::{
    MavlinkVersion, MessageData,
    dialects::ardupilotmega::{HEARTBEAT_DATA, MavComponent, MavMessage},
};
use mavlink::MavHeader;
use mavlink_codec::PacketRef;
use tracing::*;

use self::vehicle::VehicleArmGate;

pub const RAW_MAVLINK_OUT_TOPIC: &str = "mavlink_raw/out";
#[allow(unused)]
pub const RAW_MAVLINK_IN_TOPIC: &str = "mavlink_raw/in";

/// CRC-validates a borrowed frame and parses its payload straight into the concrete
/// message type `D`, skipping the large `MavMessage` enum.
fn decode<D: MessageData>(packet: &PacketRef) -> Option<D> {
    if packet.try_validate::<MavMessage>().is_err() {
        warn!(msg_id = packet.message_id(), "Frame failed CRC validation");
        return None;
    }
    let version = match packet {
        PacketRef::V1(_) => MavlinkVersion::V1,
        PacketRef::V2(_) => MavlinkVersion::V2,
    };
    match D::deser(version, packet.payload()) {
        Ok(data) => Some(data),
        Err(error) => {
            warn!(%error, msg_id = packet.message_id(), "Failed decoding frame");
            None
        }
    }
}

#[allow(unused)]
#[instrument(skip(message), level = "debug")]
pub fn encode(header: MavHeader, message: &MavMessage) -> Vec<u8> {
    let mut bytes = Vec::new();
    if let Err(error) = mavlink::write_v2_msg(&mut bytes, header, message) {
        warn!(%error, "Failed to encode MAVLink message");
    }
    bytes
}

#[instrument(skip_all, level = "trace")]
pub async fn handle_mavlink_message(bytes: &[u8], vehicle_arm: &mut VehicleArmGate) {
    // Cheap header peek first; CRC-validate only autopilot HEARTBEAT candidates.
    let Some(packet) = PacketRef::new(bytes) else {
        trace!("Not a MAVLink frame");
        return;
    };
    if packet.message_id() != HEARTBEAT_DATA::ID {
        trace!("Message skipped");
        return;
    }
    if *packet.component_id() != MavComponent::MAV_COMP_ID_AUTOPILOT1 as u8 {
        trace!("Non-autopilot HEARTBEAT skipped");
        return;
    }
    let Some(data) = decode::<HEARTBEAT_DATA>(&packet) else {
        return;
    };
    trace!("Message decoded: {data:?}");
    let _state = vehicle::on_heartbeat(vehicle_arm, &data);
}
