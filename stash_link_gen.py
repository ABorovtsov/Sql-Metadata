import posixpath


class StashLinkGenerator:
    def __init__(self, repo_stash_url, repo_local_path):
        if repo_stash_url[-1] != '/':
            repo_stash_url = repo_stash_url + '/'

        self.repo_stash_url = repo_stash_url.lower() + 'browse/'

        if repo_local_path[-2:] != '\\':
            repo_local_path = repo_local_path + '\\'

        self.repo_local_path = repo_local_path

    def get(self, file_path):
        relative_path = file_path \
            .replace(self.repo_local_path, '') \
            .replace('\\', '/') \
            .replace(' ', '%20')
        
        return (self.repo_stash_url + relative_path + '?at=refs/heads/master')
