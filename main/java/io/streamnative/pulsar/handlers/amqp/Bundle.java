package io.streamnative.pulsar.handlers.amqp;

import lombok.Data;

@Data
public class Bundle {
    private String lowBoundaries;
    private String upBoundaries;

    public Bundle(String lowBoundaries, String upBoundaries) {
        this.lowBoundaries = lowBoundaries;
        this.upBoundaries = upBoundaries;
    }

    @Override
    public int hashCode() {
        int result = lowBoundaries.hashCode();
        result = 17 * result + upBoundaries.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Bundle)) {
            return false;
        }
        Bundle bundle = (Bundle) obj;
        if (this == bundle) {
            return true;
        }
        if (bundle.lowBoundaries.equals(this.lowBoundaries) && bundle.upBoundaries.equals(this.upBoundaries)) {
            return true;
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return "Bundle(lowBoundaries=" + this.getLowBoundaries() + ", upBoundaries=" + this.getUpBoundaries() + ")";
    }

//    public boolean contains(Bundle bundle) {
//        return Integer.parseInt(this.lowBoundaries.substring(2), 16) <= Integer.parseInt(bundle.lowBoundaries.substring(2), 16)
//                && Integer.parseInt(this.upBoundaries.substring(2), 16) >= Integer.parseInt(bundle.upBoundaries.substring(2), 16);
//    }

    public boolean contains(Bundle bundle) {
        return this.lowBoundaries.substring(2).compareTo(bundle.lowBoundaries.substring(2))<=0
                && this.upBoundaries.substring(2).compareTo(bundle.upBoundaries.substring(2))>=0;
    }
}
