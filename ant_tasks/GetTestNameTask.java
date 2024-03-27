import java.util.Map;
import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Project;
import org.apache.tools.ant.Task;

public class GetTestNameTask extends Task {
    private String m_propertyName;

    public void setPropertyName(String propertyName) {
        m_propertyName = propertyName;
    }

    public void execute() throws BuildException {
        final String owningTargetName = getOwningTarget().getName();
        final String prefix = "test.";
        if (!owningTargetName.startsWith(prefix)) {
            throw new BuildException("target name must has prefix '" + prefix + "'");
        }
        if (m_propertyName == null) {
            throw new BuildException("missing 'propertyName' attribite value");
        }
        final Project project = getProject();
        project.setProperty(m_propertyName, owningTargetName.substring(prefix.length()));
    }
}
