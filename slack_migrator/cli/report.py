"""
Report generation functionality for Slack to Google Chat migration
"""

import datetime
import json
import logging
import os

import yaml

from slack_migrator.utils.logging import log_with_context


def print_dry_run_summary(migrator, report_file=None):
    """Print a summary of the dry run to the console."""
    print("\n" + "=" * 80)
    print("DRY RUN SUMMARY")
    print("=" * 80)
    print(
        f"Channels processed: {len(migrator.migration_summary['channels_processed'])}"
    )
    print(
        f"Spaces that would be created: {migrator.migration_summary['spaces_created']}"
    )
    print(
        f"Messages that would be migrated: {migrator.migration_summary['messages_created']}"
    )
    print(
        f"Reactions that would be migrated: {migrator.migration_summary['reactions_created']}"
    )
    print(
        f"Files that would be migrated: {migrator.migration_summary['files_created']}"
    )

    # Show file upload details if available
    if hasattr(migrator, "file_handler") and hasattr(
        migrator.file_handler, "get_file_statistics"
    ):
        try:
            file_stats = migrator.file_handler.get_file_statistics()
            if file_stats["total_files_processed"] > 0:
                print(f"\nFile Upload Details:")
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
        1 for _, email in migrator.user_map.items() if migrator._is_external_user(email)
    )
    if external_users > 0:
        print(f"\nExternal users detected: {external_users}")
        print("These users will be handled with external user support")

    # Get the report file path
    if report_file is None:
        # Get the output directory
        output_dir = migrator.output_dir if hasattr(migrator, "output_dir") else "."
        report_file = os.path.join(output_dir, "migration_report.yaml")

    print(f"\nDetailed report saved to {report_file}")
    print("=" * 80)
    print("\nTo perform the actual migration, run again without --dry-run")
    print("=" * 80)


def generate_report(migrator):
    """Generate a detailed migration report."""
    # Get the output directory
    output_dir = migrator.output_dir if hasattr(migrator, "output_dir") else "."

    # Set the output file path in the run directory
    report_path = os.path.join(output_dir, "migration_report.yaml")

    # Group failed messages by channel
    failed_by_channel = {}
    if hasattr(migrator, "failed_messages") and migrator.failed_messages:
        for failed_msg in migrator.failed_messages:
            channel = failed_msg.get("channel", "unknown")
            if channel not in failed_by_channel:
                failed_by_channel[channel] = []
            failed_by_channel[channel].append(failed_msg)

        # Log summary of failed messages
        log_with_context(
            logging.WARNING,
            f"Migration completed with {len(migrator.failed_messages)} failed messages across {len(failed_by_channel)} channels",
        )

        # For each channel with failures, write detailed logs
        for channel, failures in failed_by_channel.items():
            log_with_context(
                logging.WARNING,
                f"Channel {channel} had {len(failures)} failed messages",
            )

            # Write detailed failure info to channel log
            if hasattr(migrator, "output_dir"):
                logs_dir = os.path.join(migrator.output_dir, "channel_logs")
                os.makedirs(logs_dir, exist_ok=True)
                log_file = os.path.join(logs_dir, f"{channel}_migration.log")

                try:
                    # Check if file exists, append if it does
                    mode = "a" if os.path.exists(log_file) else "w"
                    with open(log_file, mode) as f:
                        f.write(f"\n\n{'='*50}\nFAILED MESSAGES DETAILS\n{'='*50}\n\n")
                        for failed_msg in failures:
                            f.write(f"Timestamp: {failed_msg.get('ts')}\n")
                            f.write(f"Error: {failed_msg.get('error')}\n")

                            # Format payload nicely if possible
                            payload = failed_msg.get("payload")
                            if payload:
                                try:
                                    f.write(
                                        f"Payload: {json.dumps(payload, indent=2)}\n"
                                    )
                                except:
                                    f.write(f"Payload: {repr(payload)}\n")

                            f.write("\n" + "-" * 40 + "\n\n")

                    log_with_context(
                        logging.INFO,
                        f"Detailed failure information for channel {channel} written to {log_file}",
                    )
                except Exception as e:
                    log_with_context(
                        logging.ERROR,
                        f"Failed to write detailed failure log for channel {channel}: {e}",
                    )

    # Create a report dictionary
    # Get detailed file statistics if available
    file_stats = {}
    if hasattr(migrator, "file_handler") and hasattr(
        migrator.file_handler, "get_file_statistics"
    ):
        try:
            file_stats = migrator.file_handler.get_file_statistics()
        except Exception as e:
            print(f"Warning: Could not retrieve detailed file statistics: {e}")

    report = {
        "migration_summary": {
            "timestamp": datetime.datetime.now().isoformat(),
            "dry_run": migrator.dry_run,
            "workspace_admin": migrator.workspace_admin,
            "export_path": str(migrator.export_root),
            "output_path": str(output_dir),
            "channels_processed": len(migrator.migration_summary["channels_processed"]),
            "spaces_created": migrator.migration_summary["spaces_created"],
            "messages_migrated": migrator.migration_summary["messages_created"],
            "reactions_migrated": migrator.migration_summary["reactions_created"],
            "files_migrated": migrator.migration_summary["files_created"],
            "failed_messages_count": len(getattr(migrator, "failed_messages", [])),
            "channels_with_failures": len(failed_by_channel),
        },
        "spaces": {},
        "skipped_channels": [],
        "failed_channels": list(failed_by_channel.keys()),
        "high_failure_rate_channels": {},
        "channel_issues": getattr(migrator, "migration_issues", {}),
        "duplicate_space_conflicts": list(
            getattr(migrator, "channel_conflicts", set())
        ),
        "users": {
            "external_users": {},
            "users_without_email": {},
        },
        "file_upload_details": file_stats,
        "recommendations": [],
    }

    # Add high failure rate channels to the report
    if hasattr(migrator, "high_failure_rate_channels"):
        report["high_failure_rate_channels"] = migrator.high_failure_rate_channels

        # Add recommendation for high failure rate channels
        if migrator.high_failure_rate_channels:
            max_failure_percentage = migrator.config.get("max_failure_percentage", 10)
            report["recommendations"].append(
                {
                    "type": "high_failure_rate",
                    "message": f"Found {len(migrator.high_failure_rate_channels)} channels with failure rates exceeding {max_failure_percentage}%. Check the detailed logs for more information.",
                    "severity": "warning",
                }
            )

    # Add recommendation for duplicate space conflicts
    if hasattr(migrator, "channel_conflicts") and migrator.channel_conflicts:
        report["recommendations"].append(
            {
                "type": "duplicate_space_conflicts",
                "message": f"Found {len(migrator.channel_conflicts)} channels with duplicate space conflicts. "
                f"These channels were skipped. Add entries to space_mapping in config.yaml to resolve: {', '.join(migrator.channel_conflicts)}",
                "severity": "error",
            }
        )

    # Add detailed info for each space
    for channel in migrator.migration_summary["channels_processed"]:
        space_name = migrator.created_spaces.get(channel)

        if not space_name:
            # Track skipped channels
            report["skipped_channels"].append(channel)
            continue

        # Get stats for this space
        space_stats = {
            "messages_migrated": 0,
            "reactions_migrated": 0,
            "files_migrated": 0,
            "external_users_allowed": False,
            "internal_users": [],
            "external_users": [],
            "failed_messages": len(failed_by_channel.get(channel, [])),
        }

        # Check if this space has external users enabled
        space_stats["external_users_allowed"] = getattr(
            migrator, "spaces_with_external_users", {}
        ).get(space_name, False)

        # Get users for this channel
        if (
            hasattr(migrator, "active_users_by_channel")
            and channel in migrator.active_users_by_channel
        ):
            active_users = migrator.active_users_by_channel[channel]

            # Process each user
            for user_id in active_users:
                user_email = migrator.user_map.get(user_id)
                if not user_email:
                    continue

                # Check if this is an external user
                if migrator._is_external_user(user_email):
                    space_stats["external_users"].append(user_email)
                else:
                    space_stats["internal_users"].append(user_email)

        # Get message stats if available
        if hasattr(migrator, "channel_stats") and channel in migrator.channel_stats:
            ch_stats = migrator.channel_stats[channel]
            space_stats["messages_migrated"] = ch_stats.get("message_count", 0)
            space_stats["reactions_migrated"] = ch_stats.get("reaction_count", 0)
            space_stats["files_migrated"] = ch_stats.get("file_count", 0)

        # Add to the report
        report["spaces"][channel] = space_stats

    # Add users without email to the report
    if hasattr(migrator, "users_without_email") and migrator.users_without_email:
        users_without_email_data = {}
        for user in migrator.users_without_email:
            user_id = user.get("id")
            if not user_id:
                continue

            user_type = (
                "Bot" if user.get("is_bot") or user.get("is_app_user") else "User"
            )
            name = user.get("name", "")
            real_name = user.get("real_name", "")

            users_without_email_data[user_id] = {
                "name": name,
                "real_name": real_name,
                "type": user_type,
            }

        report["users"]["users_without_email"] = users_without_email_data
        report["users"]["users_without_email_count"] = len(users_without_email_data)

        # Add recommendation for users without email
        if users_without_email_data:
            report["recommendations"].append(
                {
                    "type": "users_without_email",
                    "message": f"Found {len(users_without_email_data)} users without email addresses. Add them to user_mapping_overrides in your config.yaml.",
                    "severity": "warning",
                }
            )

    # Collect all external users with their Slack user IDs
    external_users = {}
    for user_id, email in migrator.user_map.items():
        if migrator._is_external_user(email):
            external_users[user_id] = email

    # Add external users to the report
    report["users"]["external_users"] = external_users
    report["users"]["external_user_count"] = len(external_users)

    # Add recommendation for external users if any
    if external_users:
        report["recommendations"].append(
            {
                "type": "external_users",
                "message": f"Found {len(external_users)} external users. Map them to internal workspace emails using user_mapping_overrides in your config.yaml.",
                "severity": "info",
            }
        )

        # Add a dedicated section for external user mappings in a format ready to copy to config.yaml
        external_mappings = []
        external_mappings.append(
            "# Copy the following section to your config.yaml under user_mapping_overrides:"
        )
        external_mappings.append("user_mapping_overrides:")
        for user_id, email in sorted(external_users.items()):
            external_mappings.append(f'  "{user_id}": ""  # {email}')
        report["external_user_mappings_for_config"] = external_mappings

    # Write the report to the output directory
    with open(report_path, "w") as f:
        yaml.dump(report, f, default_flow_style=False)

    # Log that the report was generated
    log_with_context(logging.INFO, f"Migration report generated: {report_path}")

    # Return the report file path instead of the report content
    return report_path
