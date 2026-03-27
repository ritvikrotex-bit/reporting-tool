"""
Ngrok Tunnel Launcher for MT5 P&L Studio
-----------------------------------------
Usage:
  python start_ngrok.py                     # prompts for auth token if needed
  python start_ngrok.py --token YOUR_TOKEN  # pass token directly

Steps:
  1. Sign up at https://ngrok.com (free)
  2. Copy your auth token from https://dashboard.ngrok.com/get-started/your-authtoken
  3. Run: python start_ngrok.py --token <your_token>
  4. In a separate terminal, run: streamlit run app.py
  5. Share the printed https://xxxx.ngrok-free.app URL
"""

import sys
import time
import argparse
import subprocess
import threading

def start_streamlit():
    subprocess.run([sys.executable, "-m", "streamlit", "run", "app.py", "--server.port", "8501"])

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--token", help="Ngrok auth token")
    parser.add_argument("--port", type=int, default=8501, help="Port Streamlit runs on (default 8501)")
    parser.add_argument("--no-streamlit", action="store_true", help="Don't auto-start Streamlit (tunnel only)")
    args = parser.parse_args()

    from pyngrok import ngrok, conf

    # Set auth token
    token = args.token
    if not token:
        print("\n[ngrok] No token provided.")
        print("  Get your free token at: https://dashboard.ngrok.com/get-started/your-authtoken")
        token = input("  Paste your ngrok auth token here: ").strip()

    if token:
        conf.get_default().auth_token = token
        ngrok.set_auth_token(token)

    # Optionally start Streamlit in background
    if not args.no_streamlit:
        print(f"\n[streamlit] Starting Streamlit on port {args.port}...")
        t = threading.Thread(target=start_streamlit, daemon=True)
        t.start()
        time.sleep(3)  # Give Streamlit a moment to start

    # Open ngrok tunnel
    print(f"\n[ngrok] Opening tunnel to localhost:{args.port} ...")
    tunnel = ngrok.connect(args.port, "http")
    public_url = tunnel.public_url

    print("\n" + "="*60)
    print("  PUBLIC URL (share this):")
    print(f"  {public_url}")
    print("="*60)
    print("\n  Your MT5 Reporting Tool is now accessible from anywhere.")
    print("  Press Ctrl+C to stop the tunnel.\n")

    try:
        ngrok.get_ngrok_process().proc.wait()
    except KeyboardInterrupt:
        print("\n[ngrok] Tunnel closed.")
        ngrok.kill()

if __name__ == "__main__":
    main()
