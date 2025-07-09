The plan is to make a super duper quick bittorrent client and maybe a tracker to go along with it. Implemented in Python with minimal external libraires.
Typer lib for minimal ClI tool. Reminder to use UV for faster event loop.

Features Client:
1. Endgame mode.
2. Rarest first.
3. Filter peers based on time to download for a piece (performance based priority queue).
4. Connect to all peers with multiple inflight requests.
5. Multi-file mode.
6. Optional BitTyrant + PropShare.
7. Refined Choke Unchoke strategy.

Features Tracker:
1. UDP tracker connection.
2. HTTP + HTTPS tracker connection -> no libraries. 

