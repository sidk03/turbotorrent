#!/usr/bin/env python3
"""
Simple test script for the BitTorrent client.
Downloads a small, legal torrent file to test functionality.
"""

import asyncio
import sys
from pathlib import Path
from src.torrent.parser import parse_torrent_file
from src.eventloop.client import TorrentClient
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('test_download.log')
    ]
)

async def test_download(torrent_path: Path, save_path: Path):
    """Test downloading a torrent file."""
    try:
        # Parse the torrent file
        print(f"Parsing torrent file: {torrent_path}")
        metadata = parse_torrent_file(torrent_path)
        
        print(f"Torrent: {metadata.name}")
        print(f"Total size: {metadata.total_length / (1024*1024):.2f} MB")
        print(f"Pieces: {len(metadata.pieces)}")
        print(f"Piece length: {metadata.piece_length / 1024} KB")
        
        # Create download directory
        save_path.mkdir(parents=True, exist_ok=True)
        
        # Initialize the client
        print(f"\nStarting download to: {save_path}")
        client = TorrentClient(metadata, save_path)
        
        # Start the download
        await client.start()
        
        print("\nDownload completed successfully!")
        
    except KeyboardInterrupt:
        print("\nDownload interrupted by user")
        if 'client' in locals():
            await client.cleanup()
    except Exception as e:
        print(f"Error during download: {e}")
        import traceback
        traceback.print_exc()
        if 'client' in locals():
            await client.cleanup()

def main():
    """Main entry point."""
    if len(sys.argv) < 2:
        print("Usage: python test_download.py <torrent_file> [save_directory]")
        print("\nExample:")
        print("  python test_download.py ubuntu.torrent downloads/")
        sys.exit(1)
    
    torrent_path = Path(sys.argv[1])
    
    if not torrent_path.exists():
        print(f"Error: Torrent file '{torrent_path}' not found")
        sys.exit(1)
    
    save_path = Path(sys.argv[2]) if len(sys.argv) > 2 else Path("downloads")
    
    # Run the async download
    asyncio.run(test_download(torrent_path, save_path))

if __name__ == "__main__":
    main()