# -*- coding: utf-8 -*-
from keycloak import KeycloakAdmin
import sys, getopt, os

def create_user(server_url, admin_user, admin_passwd, username, password, roles = []):
    response = None
    admin = KeycloakAdmin(server_url,
                      username=admin_user,
                      password=admin_passwd,
                      realm_name='m4i',
                      client_id='admin-cli',
                      verify=True,
                      client_secret_key=None,
                      custom_headers=None,
                      user_realm_name='master',
                      auto_refresh_token=None)
    #realms = admin.get_realms()
    #users = admin.get_users()
    payload = {"username": username,
            "email": username+"@test.com",
            "emailVerified": True,
            "enabled": True}
    res = admin.create_user(payload=payload, exist_ok=True)
    print(res)
    # if the user is new, assign the roles
    if len(res)>0:
        user_id = admin.get_user_id(username)
        available_realm_roles = admin.get_realm_roles()
        new_user_roles = [item for item in available_realm_roles if item['name'] in roles]
        atlas_user_role = [item for item in available_realm_roles if item['name'] == "ATLAS_USER"]
        new_user_roles = new_user_roles + atlas_user_role
        
        admin.assign_realm_roles(user_id=user_id, roles=new_user_roles)
        # set the password
        response = admin.set_user_password(user_id=user_id,
                                        password=password,
                                        temporary=False)
    return response

def main(argv):
    admin_user = os.environ.get('KEYCLOAK_ADMIN_USERNAME', 'admin')
    admin_passwd = os.environ.get('KEYCLOAK_ADMIN_PASSWORD', None)
    username = ''
    password_variable = ''
    roles = []
    server_url = os.environ.get('KEYCLOAK_SERVER_URL', None)
    try:
        opts, args = getopt.getopt(argv,"hu:p:r:",["username=","password_variable=","roles="])
    except getopt.GetoptError:
        print('create_keycloak_users.py -u <username> -p <environment variable with password> -r <List of roles>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('create_keycloak_users.py -u <username> -p <environment variable with password> -r <List of roles>')
            # print('create_keycloak_users.py -s <server_url> -a <admin_user> -c <admin_passwd -u <username> -p <environment variable with password> -r <List of roles>')
            sys.exit()
        # elif opt in ("-s", "--server_url"):
        #     server_url = arg
        # elif opt in ("-a", "--admin_user"):
        #     admin_user = arg
        # elif opt in ("-c", "--admin_passwd"):
        #     admin_passwd = arg
        elif opt in ("-u", "--username"):
            username = arg
        elif opt in ("-p", "--password_variable"):
            password_variable = arg
        elif opt in ("-r", "--roles"):
            roles = arg
    return server_url,admin_user,admin_passwd,username,password_variable,roles

if __name__ == "__main__":
    (server_url, admin_user, admin_passwd, username, password_variable, roles) = main(sys.argv[1:])
    try:
        password = os.environ.get(password_variable, None)
        if password == None:
            raise Exception("no password specified in enviornment")
        response = create_user(server_url, admin_user, admin_passwd, username, password, roles)
        print(response)
        print("success!")
    except Exception as e:
        print("error "+str(e))
        sys.exit(2)
