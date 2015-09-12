package avro;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Created by max2 on 8/7/15.
 */
public class GiveLogSchemaHelper {

    public static Give convertStr2AvroGive(String giveJsonStr) {

        Give ob = null;
        try {
            ob = new ObjectMapper().readValue(giveJsonStr, Give.class);

        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            e.printStackTrace();

        } catch (java.io.IOException e) {
            e.printStackTrace();
        }
        return ob;
    }
}
