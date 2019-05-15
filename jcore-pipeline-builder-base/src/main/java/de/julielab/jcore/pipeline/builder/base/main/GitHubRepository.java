package de.julielab.jcore.pipeline.builder.base.main;

public class GitHubRepository extends ComponentRepository {

    private String gitHubName;

    public GitHubRepository(String name, String version, String gitHubName) {
        super(name, version, true, "GitHubRepository");
        this.gitHubName = gitHubName;
    }

    public GitHubRepository() {
        this.type = "GitHubRepository";
    }

    public String getGitHubName() {
        return gitHubName;
    }

    public void setGitHubName(String gitHubName) {
        this.gitHubName = gitHubName;
    }
}
