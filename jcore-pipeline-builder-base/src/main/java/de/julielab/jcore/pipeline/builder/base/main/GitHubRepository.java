package de.julielab.jcore.pipeline.builder.base.main;

public class GitHubRepository extends ComponentRepository {

    private String gitHubName;

    public GitHubRepository(String name, String version, String gitHubName) {
        super(name, version, true);
        this.gitHubName = gitHubName;
    }

    public GitHubRepository() {
    }

    public String getGitHubName() {
        return gitHubName;
    }

    public void setGitHubName(String gitHubName) {
        this.gitHubName = gitHubName;
    }
}
