#!/bin/bash

# Meshtastic Tool Installation Script
# This script sets up the Meshtastic Tool with all dependencies

set -e

echo "🚀 Meshtastic Tool Installation Script"
echo "======================================"

# Check if Python 3 is installed
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 is not installed. Please install Python 3.7 or later."
    exit 1
fi

echo "✅ Python 3 found: $(python3 --version)"

# Check if pip is installed
if ! command -v pip3 &> /dev/null; then
    echo "❌ pip3 is not installed. Please install pip3."
    exit 1
fi

echo "✅ pip3 found"

# Install dependencies
echo "📦 Installing Python dependencies..."
pip3 install -r requirements.txt

# Check if config.ini exists, if not create from template
if [ ! -f "config.ini" ]; then
    echo "📝 Creating default configuration file..."
    echo "Please edit config.ini to match your Meshtastic device settings."
else
    echo "✅ Configuration file already exists"
fi

# Make the script executable
chmod +x meshconsole.py

echo ""
echo "🎉 Installation completed successfully!"
echo ""
echo "Next steps:"
echo "1. Edit config.ini to set your Meshtastic device IP address"
echo "2. Run: python3 meshconsole.py listen --ip YOUR_DEVICE_IP"
echo "3. For web interface: python3 meshconsole.py listen --ip YOUR_DEVICE_IP --web"
echo ""
echo "For help: python3 meshconsole.py --help"
echo ""
echo "🔒 Security Note: The tool automatically filters out messages from your own node."
echo "📖 See README.md for detailed usage instructions."
