#!/usr/bin/env python3
"""
TurboTorrent - Fast BitTorrent client using Python asyncio
Main entry point for the application.
"""

import asyncio
import argparse
import sys
from pathlib import Path
from src.torrent.parser import parse_torrent_file
from src.eventloop.client import TorrentClient
from src.common.logging import config_logging
import logging

logger = logging.getLogger(__name__)

async def download_torrent(torrent_path: Path, save_path: Path, verbose: bool = False):
    """
    Download a torrent file.
    
    Args:
        torrent_path: Path to the .torrent file
        save_path: Directory to save downloaded files
        verbose: Enable verbose logging
    """
    client = None
    try:
        # Configure logging level
        if verbose:
            logging.getLogger().setLevel(logging.DEBUG)
        else:
            logging.getLogger().setLevel(logging.INFO)
        
        # Parse torrent file
        logger.info(f"Parsing torrent file: {torrent_path}")
        metadata = parse_torrent_file(torrent_path)
        
        # Display torrent information
        print(f"\n{'='*60}")
        print(f"Torrent: {metadata.name}")
        print(f"Size: {metadata.total_length / (1024*1024):.2f} MB")
        print(f"Pieces: {len(metadata.pieces)} x {metadata.piece_length / 1024:.0f} KB")
        print(f"Tracker: {metadata.announce}")
        print(f"{'='*60}\n")
        
        # Create save directory if it doesn't exist
        save_path.mkdir(parents=True, exist_ok=True)
        
        # Initialize and start the client
        logger.info(f"Starting download to: {save_path}")
        client = TorrentClient(metadata, save_path)
        
        # Start the download
        await client.start()
        
        print(f"\n{'='*60}")
        print(f"✓ Download completed successfully!")
        print(f"Files saved to: {save_path}")
        print(f"{'='*60}\n")
        
    except KeyboardInterrupt:
        print("\n\n⚠ Download interrupted by user")
        if client:
            await client.cleanup()
    except Exception as e:
        logger.error(f"Download failed: {e}", exc_info=True)
        print(f"\n✗ Download failed: {e}")
        if client:
            await client.cleanup()
        sys.exit(1)

def main():
    """Main entry point for the TurboTorrent client."""
    parser = argparse.ArgumentParser(
        description="TurboTorrent - Fast BitTorrent client using Python asyncio",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s ubuntu.torrent
  %(prog)s movie.torrent -o downloads/
  %(prog)s file.torrent -v
        """
    )
    
    parser.add_argument(
        "torrent",
        type=Path,
        help="Path to the .torrent file"
    )
    
    parser.add_argument(
        "-o", "--output",
        type=Path,
        default=Path("downloads"),
        help="Output directory for downloaded files (default: downloads/)"
    )
    
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="Enable verbose logging"
    )
    
    parser.add_argument(
        "--log-file",
        type=Path,
        default=Path("turbotorrent.log"),
        help="Path to log file (default: turbotorrent.log)"
    )
    
    args = parser.parse_args()
    
    # Validate torrent file exists
    if not args.torrent.exists():
        print(f"Error: Torrent file '{args.torrent}' not found")
        sys.exit(1)
    
    # Configure logging
    config_logging(str(args.log_file))
    
    # Print startup banner
    print("\n" + "="*60)
    print("  TurboTorrent - Fast BitTorrent Client")
    print("  Using Python asyncio for maximum performance")
    print("="*60)
    
    # Run the async download
    try:
        asyncio.run(download_torrent(args.torrent, args.output, args.verbose))
    except KeyboardInterrupt:
        print("\n\nShutting down...")
        sys.exit(0)

if __name__ == "__main__":
    main()