import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;


public class YarnClientTest {
	public static void main(String[] args) throws YarnException, IOException {
		if (args.length != 2) {
			System.err.println("Expecting 2 arguments");
			System.exit(1);
		}
			
		String type = args[0];
		String app_id = null;
		String user_id = null;
		List<ApplicationId> user_applications = new ArrayList<ApplicationId>();
		
		YarnConfiguration conf = new YarnConfiguration();
		YarnClient c = YarnClient.createYarnClient();
		c.init(conf);
		c.start();
		
		if (type.equalsIgnoreCase("app_id")) {
			app_id = args[1];
			for (ApplicationReport application: c.getApplications()) {
				if (application.getApplicationId().equals(app_id))
					c.killApplication(application.getApplicationId());
			}
		} else if (type.equalsIgnoreCase("user_id")) {
			user_id = args[1];
			for (ApplicationReport application: c.getApplications()) {
				if (application.getUser().equals(user_id))
					user_applications.add(application.getApplicationId());
				if (!user_applications.isEmpty()) {
					for (ApplicationId application_id: user_applications)
						c.killApplication(application_id);
				} else {
					System.out.println("The user_id " + user_id + " does not have any applications associated");
				}
			}
		} else {
			System.err.println("Expecting either 'app_id' or 'user_id' as parameter 1");
			System.exit(1);
		}
		
	}
}
