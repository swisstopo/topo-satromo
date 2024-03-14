import configuration as config
import requests


def get_github_info():
    """
    Retrieves GitHub repository information and generates a GitHub link based on the latest commit.

    Returns:
        A dictionary containing the GitHub link. If the request fails or no commit hash is available, the link will be None.
    """
    # Enter your GitHub repository information
    owner = config.GITHUB_OWNER
    repo = config.GITHUB_REPO

    # Make a GET request to the GitHub API to retrieve information about the repository
    response = requests.get(
        f"https://api.github.com/repos/{owner}/{repo}/commits/main")

    github_info = {}

    if response.status_code == 200:
        # Extract the commit hash from the response
        commit_hash = response.json()["sha"]

        # Generate the GitHub link
        github_link = f"https://github.com/{owner}/{repo}/commit/{commit_hash}"
        github_info["GithubLink"] = github_link

    else:
        github_info["GithubLink"] = None

    # Make a GET request to the GitHub API to retrieve information about the repository releases
    response = requests.get(
        f"https://api.github.com/repos/{owner}/{repo}/releases/latest")

    if response.status_code == 200:
        # Extract the release version from the response
        release_version = response.json()["tag_name"]
    else:
        release_version = "0.0.0"

    github_info["ReleaseVersion"] = release_version

    return github_info
