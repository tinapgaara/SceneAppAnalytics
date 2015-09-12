package z_try;

/**
 * Created by TAN on 7/5/2015.
 */
public enum FeatureKind {

    MERCHANT_LIKE, MERCHANT_INTEREST, USER_INTERACTION;

    public static String toString(FeatureKind kind) {

        String result = null;

        switch (kind) {
            case MERCHANT_LIKE:
                result = "MERCHANT_LIKE";
                break;

            case MERCHANT_INTEREST:
                result = "MERCHANT_INTEREST";
                break;

            case USER_INTERACTION:
                result = "USER_INTERACTION";
                break;
        }

        return result;
    }
}
