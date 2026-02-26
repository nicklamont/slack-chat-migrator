"""
Report generation functionality for Slack to Google Chat migration
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
    from slack_migrator.core.migrator import SlackToChatMigrator


def print_dry_run_summary(
    migrator: SlackToChatMigrator, report_file: str | None = None
) -> None:
    """Print a summary of the dry run to the console.

    Args:
        migrator: The migrator whose state is summarised.
        report_file: Optional override path for the report file location.
    """
    print("\n" + "=" * 80)
    print("DRY RUN SUMMARY")
    print("=" * 80)
    print(
        f"Channels processed: {len(migrator.state.migration_summary['channels_processed'])}"
    )
    print(
        f"Spaces that would be created: {migrator.state.migration_summary['spaces_created']}"
    )
    print(
        f"Messages that would be migrated: {migrator.state.migration_summary['messages_created']}"
    )
    print(
        f"Reactions that would be migrated: {migrator.state.migration_summary['reactions_created']}"
    )
    print(
        f"Files that would be migrated: {migrator.state.migration_summary['files_created']}"
    )

    # Show file upload details if available
    if hasattr(migrator, "file_handler") and hasattr(
        migrator.file_handler, "get_file_statistics"
    ):
        try:
            file_stats = migrator.file_handler.get_file_statistics()
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
    if hasattr(migrator, "users_without_email") and migrator.users_without_email:
        print(f"\nUsers without email: {len(migrator.users_without_email)}")
        print("These users need to be mapped in config.yaml")

    # Count external users
    external_users = sum(
        1
        for _, email in migrator.user_map.items()
        if migrator.user_resolver.is_external_user(email)
    )
    if external_users > 0:
        print(f"\nExternal users detected: {external_users}")
        print("These users will be handled with external user support")

    # Get the report file path
    if report_file is None:
        # Get the output directory
        output_dir = migrator.state.output_dir or "."
        report_file = os.path.join(output_dir, "migration_report.yaml")

    print(f"\nDetailed report saved to {report_file}")
    print("=" * 80)
    print("\nTo perform the actual migration, run again without --dry-run")
    print("=" * 80)


def _group_failed_messages(
    migrator: SlackToChatMigrator,
) -> dict[str, list[FailedMessage]]:
    """Group failed messages by channel and write detailed failure logs.

    Args:
        migrator: The migrator whose state contains failed messages.

    Returns:
        Dict mapping channel names to their list of FailedMessage entries.
    """
    failed_by_channel: dict[str, list[FailedMessage]] = {}
    if not migrator.state.failed_messages:
        return failed_by_channel

    for failed_msg in migrator.state.failed_messages:
        channel = failed_msg.get("channel", "unknown")
        if channel not in failed_by_channel:
            failed_by_channel[channel] = []
        failed_by_channel[channel].append(failed_msg)

    log_with_context(
        logging.WARNING,
        f"Migration completed with {len(migrator.state.failed_messages)} failed messages across {len(failed_by_channel)} channels",
    )

    for channel, failures in failed_by_channel.items():
        log_with_context(
            logging.WARNING,
            f"Channel {channel} had {len(failures)} failed messages",
        )
        _write_failure_log(migrator.state.output_dir, channel, failures)

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
    migrator: SlackToChatMigrator,
    failed_by_channel: dict[str, list[FailedMessage]],
) -> list[dict[str, str]]:
    """Build the recommendations list for the migration report.

    Args:
        migrator: The migrator instance.
        failed_by_channel: Failed messages grouped by channel.

    Returns:
        List of recommendation dicts with type, message, and severity keys.
    """
    recommendations: list[dict[str, str]] = []

    if migrator.state.high_failure_rate_channels:
        max_pct = migrator.config.max_failure_percentage
        recommendations.append(
            {
                "type": "high_failure_rate",
                "message": f"Found {len(migrator.state.high_failure_rate_channels)} channels with failure rates exceeding {max_pct}%. Check the detailed logs for more information.",
                "severity": "warning",
            }
        )

    if migrator.state.channel_conflicts:
        recommendations.append(
            {
                "type": "duplicate_space_conflicts",
                "message": f"Found {len(migrator.state.channel_conflicts)} channels with duplicate space conflicts. "
                f"These channels were skipped. Add entries to space_mapping in config.yaml to resolve: {', '.join(migrator.state.channel_conflicts)}",
                "severity": "error",
            }
        )

    skipped_reactions = migrator.state.skipped_reactions
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
    migrator: SlackToChatMigrator,
    failed_by_channel: dict[str, list[FailedMessage]],
) -> tuple[dict[str, Any], list[str]]:
    """Build per-space stats and identify skipped channels.

    Args:
        migrator: The migrator instance.
        failed_by_channel: Failed messages grouped by channel.

    Returns:
        Tuple of (spaces dict, skipped_channels list).
    """
    spaces: dict[str, Any] = {}
    skipped_channels: list[str] = []

    for channel in migrator.state.migration_summary["channels_processed"]:
        space_name = migrator.state.created_spaces.get(channel)
        if not space_name:
            skipped_channels.append(channel)
            continue

        space_stats: dict[str, Any] = {
            "messages_migrated": 0,
            "reactions_migrated": 0,
            "files_migrated": 0,
            "external_users_allowed": migrator.state.spaces_with_external_users.get(
                space_name, False
            ),
            "internal_users": [],
            "external_users": [],
            "failed_messages": len(failed_by_channel.get(channel, [])),
        }

        if channel in migrator.state.active_users_by_channel:
            for user_id in migrator.state.active_users_by_channel[channel]:
                user_email = migrator.user_map.get(user_id)
                if not user_email:
                    continue
                if migrator.user_resolver.is_external_user(user_email):
                    space_stats["external_users"].append(user_email)
                else:
                    space_stats["internal_users"].append(user_email)

        if channel in migrator.state.channel_stats:
            ch_stats = migrator.state.channel_stats[channel]
            space_stats["messages_migrated"] = ch_stats.get("message_count", 0)
            space_stats["reactions_migrated"] = ch_stats.get("reaction_count", 0)
            space_stats["files_migrated"] = ch_stats.get("file_count", 0)

        spaces[channel] = space_stats

    return spaces, skipped_channels


def _build_user_section(
    migrator: SlackToChatMigrator,
    recommendations: list[dict[str, str]],
) -> tuple[dict[str, Any], dict[str, str]]:
    """Build the users section and external user mappings for the report.

    Args:
        migrator: The migrator instance.
        recommendations: Recommendations list to append user-related items to.

    Returns:
        Tuple of (users dict, external_users dict).
    """
    users_section: dict[str, Any] = {
        "external_users": {},
        "users_without_email": {},
    }

    # Users without email
    if hasattr(migrator, "users_without_email") and migrator.users_without_email:
        users_without_email_data: dict[str, dict[str, str]] = {}
        for user in migrator.users_without_email:
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
    for user_id, email in migrator.user_map.items():
        if migrator.user_resolver.is_external_user(email):
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


def generate_report(migrator: SlackToChatMigrator) -> str:
    """Generate a detailed migration report.

    Args:
        migrator: The migrator whose state is written to the report.

    Returns:
        Path to the generated YAML report file.
    """
    output_dir = migrator.state.output_dir or "."
    report_path = os.path.join(output_dir, "migration_report.yaml")

    failed_by_channel = _group_failed_messages(migrator)

    # File statistics
    file_stats: dict[str, Any] = {}
    if hasattr(migrator, "file_handler") and hasattr(
        migrator.file_handler, "get_file_statistics"
    ):
        try:
            file_stats = migrator.file_handler.get_file_statistics()
        except Exception as e:
            log_with_context(
                logging.WARNING,
                f"Could not retrieve detailed file statistics: {e}",
            )

    recommendations = _build_recommendations(migrator, failed_by_channel)
    spaces, skipped_channels = _build_space_details(migrator, failed_by_channel)
    users_section, external_users = _build_user_section(migrator, recommendations)

    report: dict[str, Any] = {
        "migration_summary": {
            "timestamp": datetime.datetime.now().isoformat(),
            "dry_run": migrator.dry_run,
            "workspace_admin": migrator.workspace_admin,
            "export_path": str(migrator.export_root),
            "output_path": str(output_dir),
            "channels_processed": len(
                migrator.state.migration_summary["channels_processed"]
            ),
            "spaces_created": migrator.state.migration_summary["spaces_created"],
            "messages_migrated": migrator.state.migration_summary["messages_created"],
            "reactions_migrated": migrator.state.migration_summary["reactions_created"],
            "files_migrated": migrator.state.migration_summary["files_created"],
            "failed_messages_count": len(migrator.state.failed_messages),
            "channels_with_failures": len(failed_by_channel),
        },
        "spaces": spaces,
        "skipped_channels": skipped_channels,
        "failed_channels": list(failed_by_channel.keys()),
        "high_failure_rate_channels": dict(migrator.state.high_failure_rate_channels),
        "channel_issues": migrator.state.migration_issues,
        "duplicate_space_conflicts": list(migrator.state.channel_conflicts),
        "users": users_section,
        "file_upload_details": file_stats,
        "skipped_reactions": list(migrator.state.skipped_reactions),
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
