import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.util.Collection;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.SideEffectFree;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.components.ValidationContext;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessSessionFactory;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.tika.config.TikaConfig;
import org.apache.tika.detect.Detector;
import org.apache.tika.io.TikaInputStream;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.metadata.TikaMetadataKeys;
import org.apache.tika.mime.MediaType;
import org.apache.tika.mime.MimeType;
import org.apache.tika.mime.MimeTypes;
import org.apache.tika.mime.MimeTypesFactory;
import org.apache.tika.mime.MimeTypeException;

/**
 * <p>
 * Attempts to detect the MIME Type of a FlowFile by examining its contents. If the MIME Type is determined, it is added
 * to an attribute with the name mime.type. In addition, mime.extension is set if a common file extension is known.
 * </p>
 *
 * <p>
 * MIME Type detection is performed by Apache Tika; more information about detection is available at http://tika.apache.org.
 *
 * <ul>
 * <li>application/flowfile-v3</li>
 * <li>application/flowfile-v1</li>
 * </ul>
 * </p>
 */

@EventDriven
@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
public class IdentifyCustomMimeType implements Processor {    

        public static final PropertyDescriptor USE_FILENAME_IN_DETECTION = new PropertyDescriptor.Builder()
        .displayName("Use Filename In Detection")
        .name("use-filename-in-detection")
        .description("If true will pass the filename to Tika to aid in detection.")
        .required(true)
        .allowableValues("true", "false")
        .defaultValue("true")
        .build();    
    
    public static final PropertyDescriptor MIME_CONFIG_FILE = new PropertyDescriptor.Builder()
    .displayName("Config File")
    .name("config-file")
    .required(false)
    .description("Path to MIME type config file. Only one of Config File or Config Body may be used.")
    .addValidator(new StandardValidators.FileExistsValidator(true))
    .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
    .build();    
    
    public static final PropertyDescriptor MIME_CONFIG_BODY = new PropertyDescriptor.Builder()
    .displayName("Config Body")
    .name("config-body")
    .required(false)
    .description("Body of MIME type config file. Only one of Config File or Config Body may be used.")
    .addValidator(Validator.VALID)
    .expressionLanguageSupported(ExpressionLanguageScope.NONE)
    .build();    
    
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
    .name("success")
    .description("All FlowFiles are routed to success")
    .build();    
    
    private Set<Relationship> relationships;
    
    private List<PropertyDescriptor> properties;    
    private TikaConfig config;
    private Detector detector;
    private MimeTypes mimeTypes;    
    
    
    @Override
    public void initialize(final ProcessorInitializationContext context) {        
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(USE_FILENAME_IN_DETECTION);
        properties.add(MIME_CONFIG_BODY);
        properties.add(MIME_CONFIG_FILE);
        this.properties = Collections.unmodifiableList(properties);        
        final Set<Relationship> rels = new HashSet<>();
        rels.add(REL_SUCCESS);
        this.relationships = Collections.unmodifiableSet(rels);
        this.config = TikaConfig.getDefaultConfig();
    }    
    
    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }    
    
    @Override
    public List<PropertyDescriptor> getPropertyDescriptors() {
        return properties;
    }
    
    @Override
    public PropertyDescriptor getPropertyDescriptor(String name) { return null }
    
    @Override
    public void onTrigger(final ProcessContext context, final ProcessSessionFactory sessionFactory) {
        def session = sessionFactory.createSession();
        FlowFile flowFile = session.get();

        if (flowFile == null) {
            return;
        }        

        if(this.detector == null) {    

            String configBody = context.getProperty(MIME_CONFIG_BODY).getValue();
            String configFile = context.getProperty(MIME_CONFIG_FILE).evaluateAttributeExpressions().getValue();        	
            if (configBody == null && configFile == null){
                this.detector = config.getDetector();
                this.mimeTypes = config.getMimeRepository();

            } else if (configBody != null) {
                try {
                    this.detector = MimeTypesFactory.create(new ByteArrayInputStream(configBody.getBytes()));
                    this.mimeTypes = (MimeTypes)this.detector;

                } catch (Exception e) {
                    context.yield();
                    throw new ProcessException("Failed to load config body", e);
                }        	} else {
                try {
                    this.detector = MimeTypesFactory.create(new FileInputStream(configFile));
                    this.mimeTypes = (MimeTypes)this.detector;
                } catch (Exception e) {
                    context.yield();
                    throw new ProcessException("Failed to load config file", e);
                }
            }
        }

        final AtomicReference<String> mimeTypeRef = new AtomicReference<>(null);
        final String filename = flowFile.getAttribute(CoreAttributes.FILENAME.key());        
        
        session.read(flowFile,
            { 
        	inputStream ->      

                TikaInputStream tikaStream = TikaInputStream.get(inputStream);
                Metadata metadata = new Metadata();                    
                if (filename != null && context.getProperty(USE_FILENAME_IN_DETECTION).asBoolean()) {
                    metadata.add(TikaMetadataKeys.RESOURCE_NAME_KEY, filename);
                }

                // Get mime type
                MediaType mediatype = detector.detect(tikaStream, metadata);

                mimeTypeRef.set(mediatype.toString());        
            } as InputStreamCallback)
        

        String mimeType = mimeTypeRef.get();
        String extension = "";
        try {

            MimeType mimetype;
            mimetype = mimeTypes.forName(mimeType);
            extension = mimetype.getExtension();

        } catch (MimeTypeException ex) {        
        }        
        
        // Workaround for bug in Tika - https://issues.apache.org/jira/browse/TIKA-1563
        if (mimeType != null && mimeType.equals("application/gzip") && extension.equals(".tgz")) {
            extension = ".gz";
        }        
        
        if (mimeType == null) {
            flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), "application/octet-stream");
            flowFile = session.putAttribute(flowFile, "mime.extension", "");
        } else {
            flowFile = session.putAttribute(flowFile, CoreAttributes.MIME_TYPE.key(), mimeType);
            flowFile = session.putAttribute(flowFile, "mime.extension", extension);
        }        
        
        session.getProvenanceReporter().modifyAttributes(flowFile);

        session.transfer(flowFile, REL_SUCCESS);
        session.commit();

    }    
    
    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
    	this.detector = null;
    }
    
    @Override
    public Collection<ValidationResult> validate(final ValidationContext context) {
        return Collections.emptySet();
    }
        
    @Override
    String getIdentifier() { return null }
    
}
processor = new IdentifyCustomMimeType()