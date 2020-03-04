package kettle.java.exec.src;

import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.plugins.PluginFolder;
import org.pentaho.di.core.plugins.StepPluginType;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;

public class KettleJavaClass {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		String file = "src/main/resources/CONECEL.ktr";

		try {
			//StepPluginType.getInstance().getPluginFolders().add(new PluginFolder("C:\\Program Files\\data-integration\\plugins", false, true));
			KettleEnvironment.init();
			TransMeta metadata = new TransMeta(file);
			Trans trans = new Trans(metadata);

			trans.execute(null);
			trans.waitUntilFinished();

			if (trans.getErrors() > 0) {
				System.out.println("Error Executing Transformation");
			}

		} catch (KettleException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

}
