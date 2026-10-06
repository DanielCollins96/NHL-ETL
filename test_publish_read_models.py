#!/usr/bin/env python3
import unittest
from unittest.mock import patch

from publish_read_models_to_s3 import (
    DEFAULT_INVALIDATION_MAX_PATHS,
    collapse_invalidation_paths,
    invalidate_cloudfront,
    invalidation_paths_for_uploads,
    normalize_invalidation_mode,
)


class NormalizeInvalidationModeTests(unittest.TestCase):
    def test_aliases(self):
        self.assertEqual(normalize_invalidation_mode("none"), "none")
        self.assertEqual(normalize_invalidation_mode(""), "none")
        self.assertEqual(normalize_invalidation_mode("prefix"), "prefix")
        self.assertEqual(normalize_invalidation_mode("batch"), "prefix")
        self.assertEqual(normalize_invalidation_mode("changed"), "prefix")
        self.assertEqual(normalize_invalidation_mode("wildcard"), "wildcard")

    def test_rejects_unknown(self):
        with self.assertRaises(ValueError):
            normalize_invalidation_mode("keys")


class CollapseInvalidationPathsTests(unittest.TestCase):
    def test_small_playing_window_stays_exact(self):
        keys = [
            "games/2024020001.json",
            "games/dates/2026-10-05.json",
            "players/8478402.json",
            "players/8478403.json",
            "teams/1.json",
            "seasons/20252026.json",
            "indexes/teams.json",
        ]
        paths = collapse_invalidation_paths(keys)
        self.assertEqual(paths, [f"/{key}" for key in sorted(keys)])
        self.assertNotIn("/*", paths)
        self.assertNotIn("/players/*", paths)

    def test_many_players_collapse_not_per_object(self):
        keys = [f"players/{8470000 + i}.json" for i in range(80)]
        keys.extend(
            [
                "games/2024020001.json",
                "games/2024020002.json",
                "games/dates/2026-10-05.json",
                "teams/1.json",
                "seasons/20252026.json",
                "indexes/teams.json",
                "drafts/2024.json",
            ]
        )
        paths = collapse_invalidation_paths(keys, max_paths=24)
        self.assertIn("/players/*", paths)
        self.assertTrue(all(not p.startswith("/players/") or p == "/players/*" for p in paths))
        self.assertIn("/games/2024020001.json", paths)
        self.assertIn("/games/2024020002.json", paths)
        self.assertIn("/games/dates/2026-10-05.json", paths)
        self.assertIn("/seasons/20252026.json", paths)
        self.assertNotIn("/*", paths)
        self.assertLessEqual(len(paths), 24)
        # Unrelated drafts are only present because this test included one uploaded key.
        self.assertIn("/drafts/2024.json", paths)

    def test_unchanged_players_do_not_force_player_prefix(self):
        keys = [
            "games/2024020001.json",
            "indexes/teams.json",
            "seasons/20252026.json",
        ]
        paths = collapse_invalidation_paths(keys)
        self.assertEqual(
            paths,
            [
                "/games/2024020001.json",
                "/indexes/teams.json",
                "/seasons/20252026.json",
            ],
        )
        self.assertNotIn("/players/*", paths)
        self.assertNotIn("/drafts/*", paths)

    def test_does_not_include_drafts_when_not_uploaded(self):
        keys = [f"players/{i}.json" for i in range(40)] + [
            f"games/{2024020000 + i}.json" for i in range(12)
        ]
        paths = collapse_invalidation_paths(keys, max_paths=24)
        self.assertTrue(all("draft" not in path for path in paths))
        self.assertNotIn("/*", paths)

    def test_bucket_prefix_is_preserved_and_not_fully_purged(self):
        keys = [f"hockey-read-models/players/{i}.json" for i in range(40)]
        keys.append("hockey-read-models/games/2024020001.json")
        paths = collapse_invalidation_paths(
            keys, bucket_prefix="hockey-read-models", max_paths=24
        )
        self.assertIn("/hockey-read-models/players/*", paths)
        self.assertIn("/hockey-read-models/games/2024020001.json", paths)
        self.assertNotIn("/*", paths)
        self.assertNotIn("/hockey-read-models/*", paths)

    def test_player_search_collapses_before_all_indexes(self):
        keys = [f"indexes/player-search/{i:02x}.json" for i in range(30)]
        keys.extend(
            [
                "indexes/teams.json",
                "indexes/player-ids.json",
                "games/2024020001.json",
            ]
        )
        paths = collapse_invalidation_paths(keys, max_paths=24)
        self.assertIn("/indexes/player-search/*", paths)
        self.assertIn("/indexes/teams.json", paths)
        self.assertIn("/indexes/player-ids.json", paths)
        self.assertIn("/games/2024020001.json", paths)
        self.assertNotIn("/indexes/*", paths)
        self.assertNotIn("/*", paths)

    def test_respects_max_paths_without_full_wildcard(self):
        keys = [f"players/{i}.json" for i in range(50)]
        keys.extend(f"teams/{i}.json" for i in range(16))
        keys.extend(f"games/{2024020000 + i}.json" for i in range(12))
        keys.extend(
            [
                "indexes/teams.json",
                "indexes/player-ids.json",
                "indexes/team-ids.json",
                "indexes/team-rosters.json",
                "indexes/game-date-range.json",
                "seasons/20252026.json",
            ]
        )
        paths = collapse_invalidation_paths(keys, max_paths=24)
        self.assertLessEqual(len(paths), 24)
        self.assertIn("/players/*", paths)
        self.assertNotIn("/*", paths)
        self.assertTrue(any(path.startswith("/games/") for path in paths))
        self.assertIn("/seasons/20252026.json", paths)


class InvalidationPathsForUploadsTests(unittest.TestCase):
    def test_wildcard_mode_is_explicit_full_purge(self):
        self.assertEqual(
            invalidation_paths_for_uploads(["players/1.json"], "wildcard"),
            ["/*"],
        )

    def test_none_mode_is_empty(self):
        self.assertEqual(invalidation_paths_for_uploads(["players/1.json"], "none"), [])

    def test_batch_alias_uses_prefix_collapse(self):
        keys = [f"players/{i}.json" for i in range(40)] + ["games/1.json"]
        paths = invalidation_paths_for_uploads(keys, "batch")
        self.assertIn("/players/*", paths)
        self.assertIn("/games/1.json", paths)
        self.assertNotIn("/*", paths)


class InvalidateCloudfrontTests(unittest.TestCase):
    @patch("publish_read_models_to_s3.boto3.client")
    def test_prefix_mode_sends_collapsed_batch(self, mock_client):
        cf = mock_client.return_value
        keys = [f"players/{i}.json" for i in range(40)] + ["games/2024020001.json"]
        invalidate_cloudfront("DIST", "prefix", keys)
        cf.create_invalidation.assert_called_once()
        batch = cf.create_invalidation.call_args.kwargs["InvalidationBatch"]
        items = batch["Paths"]["Items"]
        self.assertEqual(batch["Paths"]["Quantity"], len(items))
        self.assertIn("/players/*", items)
        self.assertIn("/games/2024020001.json", items)
        self.assertNotIn("/*", items)
        self.assertLessEqual(len(items), DEFAULT_INVALIDATION_MAX_PATHS)

    @patch("publish_read_models_to_s3.boto3.client")
    def test_missing_distribution_is_noop(self, mock_client):
        invalidate_cloudfront("", "prefix", ["players/1.json"])
        mock_client.assert_not_called()

    @patch("publish_read_models_to_s3.boto3.client")
    def test_none_mode_is_noop(self, mock_client):
        invalidate_cloudfront("DIST", "none", ["players/1.json"])
        mock_client.assert_not_called()


if __name__ == "__main__":
    unittest.main()
