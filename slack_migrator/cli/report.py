"""
Report generation functionality for Slack to Google Chat migration.
"""

from __future__ import annotations

import datetime
import json
import logging
import os
from typing import TYPE_CHECKING, Any

import yaml

from slack_migrator.types import FailedMessage
from slack_migrator.utils.logging import log_with_context

if TYPE_CHECKING:
    from slack_migrator.core.config import MigrationConfig
    from slack_migrator.core.context import MigrationContext
    from slack_migrator.core.state import MigrationState


def print_dry_run_summary(
    ctx: MigrationContext,
    state: MigrationState,
    user_resolver: Any,
    file_handler: Any | None = None,
    report_file: str | None = None,
) -> None:
    """Print a summary of the dry run to the console.

    Args:
        ctx: Immutable migration context.
        state: Mutable migration state.
        user_resolver: User identity resolver.
        file_handler: Optional file handler with statistics.
        report_file: Optional override path for the report file location.
    """
    print("\n" + "=" * 80)
    print("DRY RUN SUMMARY")
    print("=" * 80)
    print(
        f"Channels processed: {len(state.progress.migration_summary['channels_processed'])}"
    )
    print(
        f"Spaces that would be created: {state.progress.migration_summary['spaces_created']}"
    )
    print(
        f"Messages that would be migrated: {state.progress.migration_summary['messages_created']}"
    )
    print(
        f"Reactions that would be migrated: {state.progress.migration_summary['reactions_created']}"
    )
    print(
        f"Files that would be migrated: {state.progress.migration_summary['files_created']}"
    )

    # Show file upload details if available
    if file_handler is not None and hasattr(file_handler, "get_file_statistics"):
        try:
            file_stats = file_handler.get_file_statistics()
            if file_stats["total_files_processed"] > 0:
                print("\nFile Upload Details:")
                print(f"  Total files processed: {file_stats['total_files_processed']}")
                print(f"  Successful uploads: {file_stats['successful_uploads']}")
                print(f"  Failed uploads: {file_stats['failed_uploads']}")
                print(f"  Drive uploads: {file_stats['drive_uploads']}")
                print(f"  Direct uploads: {file_stats['direct_uploads']}")
                print(f"  External user files: {file_stats['external_user_files']}")
                print(f"  Ownership transferred: {file_stats['ownership_transferred']}")
                print(f"  Success rate: {file_stats['success_rate']:.1f}%")
        except Exception as e:
            print(f"  (Could not retrieve detailed file statistics: {e})")

    # Show users without email
    if ctx.users_without_email:
        print(f"\nUsers without email: {len(ctx.users_without_email)}")
        print("These users need to be mapped in config.yaml")

    # Count external users
    external_users = sum(
        1 for _, email in ctx.user_map.items() if user_resolver.is_external_user(email)
    )
    if external_users > 0:
        print(f"\nExternal users detected: {external_users}")
        print("These users will be handled with external user support")

    # Get the report file path
    if report_file is None:
        # Get the output directory
        output_dir = state.context.output_dir or "."
        report_file = os.path.join(output_dir, "migration_report.yaml")

    print(f"\nDetailed report saved to {report_file}")
    print("=" * 80)
    print("\nTo perform the actual migration, run again without --dry-run")
    print("=" * 80)


def _group_failed_messages(
    state: MigrationState,
) -> dict[str, list[FailedMessage]]:
    """Group failed messages by channel and write detailed failure logs.

    Args:
        state: Migration state containing failed messages.

    Returns:
        Dict mapping channel names to their list of FailedMessage entries.
    """
    failed_by_channel: dict[str, list[FailedMessage]] = {}
    if not state.messages.failed_messages:
        return failed_by_channel

    for failed_msg in state.messages.failed_messages:
        channel = failed_msg.get("channel", "unknown")
        if channel not in failed_by_channel:
            failed_by_channel[channel] = []
        failed_by_channel[channel].append(failed_msg)

    log_with_context(
        logging.WARNING,
        f"Migration completed with {len(state.messages.failed_messages)} failed messages across {len(failed_by_channel)} channels",
    )

    for channel, failures in failed_by_channel.items():
        log_with_context(
            logging.WARNING,
            f"Channel {channel} had {len(failures)} failed messages",
        )
        _write_failure_log(state.context.output_dir, channel, failures)

    return failed_by_channel


def _write_failure_log(
    output_dir: str | None,
    channel: str,
    failures: list[FailedMessage],
) -> None:
    """Write detailed failure information to a channel-specific log file.

    Args:
        output_dir: Base output directory for logs, or None to skip.
        channel: Channel name for the log file.
        failures: List of failed message entries to write.
    """
    if not output_dir:
        return

    logs_dir = os.path.join(output_dir, "channel_logs")
    os.makedirs(logs_dir, exist_ok=True)
    log_file = os.path.join(logs_dir, f"{channel}_migration.log")

    try:
        mode = "a" if os.path.exists(log_file) else "w"
        with open(log_file, mode) as f:
            f.write(f"\n\n{'=' * 50}\nFAILED MESSAGES DETAILS\n{'=' * 50}\n\n")
            for failed_msg in failures:
                f.write(f"Timestamp: {failed_msg.get('ts')}\n")
                f.write(f"Error: {failed_msg.get('error')}\n")
                payload = failed_msg.get("payload")
                if payload:
                    try:
                        f.write(f"Payload: {json.dumps(payload, indent=2)}\n")
                    except (TypeError, ValueError):
                        f.write(f"Payload: {payload!r}\n")
                f.write("\n" + "-" * 40 + "\n\n")

        log_with_context(
            logging.INFO,
            f"Detailed failure information for channel {channel} written to {log_file}",
        )
    except OSError as e:
        log_with_context(
            logging.ERROR,
            f"Failed to write detailed failure log for channel {channel}: {e}",
        )


def _build_recommendations(
    state: MigrationState,
    config: MigrationConfig,
    failed_by_channel: dict[str, list[FailedMessage]],
) -> list[dict[str, str]]:
    """Build the recommendations list for the migration report.

    Args:
        state: Migration state.
        config: Migration configuration.
        failed_by_channel: Failed messages grouped by channel.

    Returns:
        List of recommendation dicts with type, message, and severity keys.
    """
    recommendations: list[dict[str, str]] = []

    if state.errors.high_failure_rate_channels:
        max_pct = config.max_failure_percentage
        recommendations.append(
            {
                "type": "high_failure_rate",
                "message": f"Found {len(state.errors.high_failure_rate_channels)} channels with failure rates exceeding {max_pct}%. Check the detailed logs for more information.",
                "severity": "warning",
            }
        )

    if state.errors.channel_conflicts:
        recommendations.append(
            {
                "type": "duplicate_space_conflicts",
                "message": f"Found {len(state.errors.channel_conflicts)} channels with duplicate space conflicts. "
                f"These channels were skipped. Add entries to space_mapping in config.yaml to resolve: {', '.join(state.errors.channel_conflicts)}",
                "severity": "error",
            }
        )

    skipped_reactions = state.users.skipped_reactions
    if skipped_reactions:
        recommendations.append(
            {
                "type": "skipped_reactions",
                "message": f"Skipped {len(skipped_reactions)} reactions from unmapped users. "
                "Add these users to user_mapping_overrides in config.yaml to include their reactions.",
                "severity": "warning",
            }
        )

    return recommendations


def _build_space_details(
    state: MigrationState,
    user_map: dict[str, str],
    user_resolver: Any,
    failed_by_channel: dict[str, list[FailedMessage]],
) -> tuple[dict[str, Any], list[str]]:
    """Build per-space stats and identify skipped channels.

    Args:
        state: Migration state.
        user_map: Slack user ID to Google email mapping.
        user_resolver: User identity resolver.
        failed_by_channel: Failed messages grouped by channel.

    Returns:
        Tuple of (spaces dict, skipped_channels list).
    """
    spaces: dict[str, Any] = {}
    skipped_channels: list[str] = []

    for channel in state.progress.migration_summary["channels_processed"]:
        space_name = state.spaces.created_spaces.get(channel)
        if not space_name:
            skipped_channels.append(channel)
            continue

        space_stats: dict[str, Any] = {
            "messages_migrated": 0,
            "reactions_migrated": 0,
            "files_migrated": 0,
            "external_users_allowed": state.progress.spaces_with_external_users.get(
                space_name, False
            ),
            "internal_users": [],
            "external_users": [],
            "failed_messages": len(failed_by_channel.get(channel, [])),
        }

        if channel in state.progress.active_users_by_channel:
            for user_id in state.progress.active_users_by_channel[channel]:
                user_email = user_map.get(user_id)
                if not user_email:
                    continue
                if user_resolver.is_external_user(user_email):
                    space_stats["external_users"].append(user_email)
                else:
                    space_stats["internal_users"].append(user_email)

        if channel in state.progress.channel_stats:
            ch_stats = state.progress.channel_stats[channel]
            space_stats["messages_migrated"] = ch_stats.get("message_count", 0)
            space_stats["reactions_migrated"] = ch_stats.get("reaction_count", 0)
            space_stats["files_migrated"] = ch_stats.get("file_count", 0)

        spaces[channel] = space_stats

    return spaces, skipped_channels


def _build_user_section(
    user_map: dict[str, str],
    user_resolver: Any,
    users_without_email: list[dict[str, Any]],
    recommendations: list[dict[str, str]],
) -> tuple[dict[str, Any], dict[str, str]]:
    """Build the users section and external user mappings for the report.

    Args:
        user_map: Slack user ID to Google email mapping.
        user_resolver: User identity resolver.
        users_without_email: List of user dicts without email addresses.
        recommendations: Recommendations list to append user-related items to.

    Returns:
        Tuple of (users dict, external_users dict).
    """
    users_section: dict[str, Any] = {
        "external_users": {},
        "users_without_email": {},
    }

    # Users without email
    if users_without_email:
        users_without_email_data: dict[str, dict[str, str]] = {}
        for user in users_without_email:
            uid = user.get("id")
            if not uid:
                continue
            user_type = (
                "Bot" if user.get("is_bot") or user.get("is_app_user") else "User"
            )
            users_without_email_data[uid] = {
                "name": user.get("name", ""),
                "real_name": user.get("real_name", ""),
                "type": user_type,
            }
        users_section["users_without_email"] = users_without_email_data
        users_section["users_without_email_count"] = len(users_without_email_data)

        if users_without_email_data:
            recommendations.append(
                {
                    "type": "users_without_email",
                    "message": f"Found {len(users_without_email_data)} users without email addresses. Add them to user_mapping_overrides in your config.yaml.",
                    "severity": "warning",
                }
            )

    # External users
    external_users: dict[str, str] = {}
    for user_id, email in user_map.items():
        if user_resolver.is_external_user(email):
            external_users[user_id] = email

    users_section["external_users"] = external_users
    users_section["external_user_count"] = len(external_users)

    if external_users:
        recommendations.append(
            {
                "type": "external_users",
                "message": f"Found {len(external_users)} external users. Map them to internal workspace emails using user_mapping_overrides in your config.yaml.",
                "severity": "info",
            }
        )

    return users_section, external_users


def generate_report(
    ctx: MigrationContext,
    state: MigrationState,
    user_resolver: Any,
    file_handler: Any | None = None,
) -> str:
    """Generate a detailed migration report.

    Args:
        ctx: Immutable migration context.
        state: Mutable migration state.
        user_resolver: User identity resolver.
        file_handler: Optional file handler with statistics.

    Returns:
        Path to the generated YAML report file.
    """
    output_dir = state.context.output_dir or "."
    report_path = os.path.join(output_dir, "migration_report.yaml")

    failed_by_channel = _group_failed_messages(state)

    # File statistics
    file_stats: dict[str, Any] = {}
    if file_handler is not None and hasattr(file_handler, "get_file_statistics"):
        try:
            file_stats = file_handler.get_file_statistics()
        except Exception as e:
            log_with_context(
                logging.WARNING,
                f"Could not retrieve detailed file statistics: {e}",
            )

    recommendations = _build_recommendations(state, ctx.config, failed_by_channel)
    spaces, skipped_channels = _build_space_details(
        state, ctx.user_map, user_resolver, failed_by_channel
    )
    users_section, external_users = _build_user_section(
        ctx.user_map, user_resolver, ctx.users_without_email, recommendations
    )

    report: dict[str, Any] = {
        "migration_summary": {
            "timestamp": datetime.datetime.now().isoformat(),
            "dry_run": ctx.dry_run,
            "workspace_admin": ctx.workspace_admin,
            "export_path": str(ctx.export_root),
            "output_path": str(output_dir),
            "channels_processed": len(
                state.progress.migration_summary["channels_processed"]
            ),
            "spaces_created": state.progress.migration_summary["spaces_created"],
            "messages_migrated": state.progress.migration_summary["messages_created"],
            "reactions_migrated": state.progress.migration_summary["reactions_created"],
            "files_migrated": state.progress.migration_summary["files_created"],
            "failed_messages_count": len(state.messages.failed_messages),
            "channels_with_failures": len(failed_by_channel),
        },
        "spaces": spaces,
        "skipped_channels": skipped_channels,
        "failed_channels": list(failed_by_channel.keys()),
        "high_failure_rate_channels": dict(state.errors.high_failure_rate_channels),
        "channel_issues": state.errors.migration_issues,
        "duplicate_space_conflicts": list(state.errors.channel_conflicts),
        "users": users_section,
        "file_upload_details": file_stats,
        "skipped_reactions": list(state.users.skipped_reactions),
        "recommendations": recommendations,
    }

    # External user mappings ready for config.yaml
    if external_users:
        external_mappings = [
            "# Copy the following section to your config.yaml under user_mapping_overrides:",
            "user_mapping_overrides:",
        ]
        for user_id, email in sorted(external_users.items()):
            external_mappings.append(f'  "{user_id}": ""  # {email}')
        report["external_user_mappings_for_config"] = external_mappings

    with open(report_path, "w") as f:
        yaml.dump(report, f, default_flow_style=False)

    log_with_context(logging.INFO, f"Migration report generated: {report_path}")
    return report_path
