import socket
import requests
import subprocess

def get_local_ip():
    try:
        # Execute ifconfig command to get IP address of eth0 interface
        result = subprocess.run(['ifconfig', 'eth0'], capture_output=True, text=True)
        
        # Extract IP address from the output
        output_lines = result.stdout.split('\n')
        for line in output_lines:
            if 'inet ' in line:
                interface_ip = line.split()[1]
                return interface_ip
    except Exception as e:
        print(f"Error: {e}")
        return None

def get_public_ip():
    # Get public IP address using an external service
    response = requests.get('https://api.ipify.org').text
    return response

def read_credentials_from_file(file_path):
    with open(file_path, 'r') as file:
        user, password = file.readline().strip().split(':')
    return user, password

def send_sms(local_ip, public_ip, user, password):
    # Compose message
    message = f"Local IP: {local_ip}, Public IP: {public_ip}"

    # Send SMS using URL
    url = f"https://smsapi.free-mobile.fr/sendmsg?user={user}&pass={password}&msg={message}"
    response = requests.get(url)
    
    # Check if SMS was sent successfully
    if response.status_code == 200:
        print("SMS sent successfully!")
    else:
        print("Failed to send SMS.")

if __name__ == "__main__":
    local_ip = get_local_ip()
    public_ip = get_public_ip()
    user, password = read_credentials_from_file('credentials.txt')
    send_sms(local_ip, public_ip, user, password)
