import random

def generate_random_ip(ip_type, min_value, max_value):
    if ip_type == 'IPv4':
        return '.'.join(str(random.randint(min_value, max_value)) for _ in range(4))
    elif ip_type == 'IPv6':
        return ':'.join('{:04x}'.format(random.randint(min_value, max_value)) for _ in range(8))

def generate_random_port():
    return random.randint(1, 65535)

def generate_random_ip_type():
    return random.choice(['IPv4', 'IPv6'])


min_ip_value = 1
max_ip_value = 255
mp = {}
cnt=0
while(cnt<50000):
    ip_type=generate_random_ip_type()
    src_port = generate_random_port()
    dest_port = generate_random_port()
    src_ip = generate_random_ip(ip_type,min_ip_value,max_ip_value)
    dest_ip = generate_random_ip(ip_type,min_ip_value,max_ip_value)
    res = src_ip+","+dest_ip+","+str(src_port)+","+str(dest_port)+","+ip_type+"\n"
    reverse_res = dest_ip+","+src_ip+","+str(dest_port)+","+str(src_port)+","+ip_type+"\n"
    if res not in mp.keys() and reverse_res not in mp.keys():
        #push res and reverse res into the csv file
        mp[res] = 1
        mp[reverse_res] = 1
        with open("data_100000_tuples.csv",mode="a")as file:
            file.write(res)
            file.write(reverse_res)
        cnt+=1
    
    