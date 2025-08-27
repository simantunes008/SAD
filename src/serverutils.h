#ifndef SERVERUTILS_H
#define SERVERUTILS_H

#define MAX_DATANODES 10
#define MAX_REPLICAS 3
#define MAX_PATH 512
#define SPATH "/home/vagrant/server/"

//replaces path given by client (fuse) (e.g., /backend/file1.txt with /home/vagrant/server/file1.txt)
void handle_path(char* oldpath, char* newpath) {
    char *token, *string, *tofree;
    tofree = string = strdup(oldpath);
    
    char relative_path[MAX_PATH] = "";
    int count = 0;
    
    while ((token = strsep(&string, "/")) != NULL) {
        if (strlen(token) > 0) {
            count++;
            if (count > 1) { // Pular o primeiro componente (/backend)
                if (strlen(relative_path) > 0) {
                    strcat(relative_path, "/");
                }
                strcat(relative_path, token);
            }
        }
    }
    
    snprintf(newpath, MAX_PATH, "%s%s", SPATH, relative_path);
    free(tofree);
}

#endif // SERVERUTILS_H