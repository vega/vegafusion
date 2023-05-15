use std::sync::Arc;
// This is the interface to the JVM that we'll call the majority of our
// methods on.
use jni::JNIEnv;

// These objects are what you should use as arguments to your native
// function. They carry extra lifetime information to prevent them escaping
// this context and getting used after being GC'd.
use jni::objects::{JClass, JObject, JString};

// This is just a pointer. We'll be returning it from our function. We
// can't return one of the objects with lifetime information because the
// lifetime checker won't let us.
use jni::sys::{jlong, jstring};
use vegafusion_common::error::Result;
use vegafusion_runtime::task_graph::runtime::VegaFusionRuntime;
use vegafusion_sql::connection::Connection;
use vegafusion_sql::connection::datafusion_conn::DataFusionConnection;
use std::panic;
use vegafusion_core::patch::patch_pre_transformed_spec;
use vegafusion_core::spec::chart::ChartSpec;

struct VegaFusionRuntimeState {
    pub vf_runtime: VegaFusionRuntime,
    pub tokio_runtime: tokio::runtime::Runtime,
}

// This keeps Rust from "mangling" the name and making it unique for this
// crate.
#[no_mangle]
pub extern "system" fn Java_io_vegafusion_VegaFusionRuntime_hello<'local> (
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    input: JString<'local>,
) -> jstring {
    // First, we have to get the string out of Java. Check out the `strings`
    // module for more info on how this works.
    let input: String = env
        .get_string(&input)
        .expect("Couldn't get java string!")
        .into();

    // Then we have to create a new Java string to return. Again, more info
    // in the `strings` module.
    let output = env
        .new_string(format!("Hello, {}!", input))
        .expect("Couldn't create java string!");

    // Finally, extract the raw pointer to return.
    output.into_raw()
}

#[no_mangle]
pub extern "system" fn Java_io_vegafusion_VegaFusionRuntime_version<'local> (
    env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jstring {
    let version = env!("CARGO_PKG_VERSION");

    // Then we have to create a new Java string to return. Again, more info
    // in the `strings` module.
    let output = env
        .new_string(version)
        .expect("Couldn't create java string!");

    // Finally, extract the raw pointer to return.
    output.into_raw()
}

#[no_mangle]
pub extern "system" fn Java_io_vegafusion_VegaFusionRuntime_innerCreate<'local> (
    env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jlong {
    let result = panic::catch_unwind(|| {
        inner_create()
    });

    match result {
        Ok(Ok(state)) => {
            Box::into_raw(Box::new(state)) as jlong
        }
        Ok(Err(vf_err)) => {
            todo!("Raise VegaFusion Error")
        }
        Err(unwind_err) => {
            todo!("Raise Panic Error")
        }
    }
}

fn inner_create() -> Result<VegaFusionRuntimeState> {
    // Use DataFusion connection and multi-threaded tokio runtime
    let conn = Arc::new(DataFusionConnection::default()) as Arc<dyn Connection>;
    let capacity = 64;
    let memory_limit = 2 << 30;  // 2GB
    let vf_runtime = VegaFusionRuntime::new(conn, Some(capacity), Some(memory_limit));

    // Build tokio runtime
    let mut builder = tokio::runtime::Builder::new_multi_thread();
    builder.enable_all();
    let worker_threads = 4;
    builder.worker_threads(worker_threads.max(1) as usize);
    let tokio_runtime = builder.build()?;

    Ok(VegaFusionRuntimeState {
        vf_runtime,
        tokio_runtime,
    })
}

#[no_mangle]
pub unsafe extern "system" fn Java_io_vegafusion_VegaFusionRuntime_innerDestroy<'local> (
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    state_ptr: jlong,
) {
    // Cast/Box the state_ptr so that the drop logic will run
    let _boxed_state = Box::from_raw(state_ptr as *mut VegaFusionRuntimeState);
}

#[no_mangle]
pub unsafe extern "system" fn Java_io_vegafusion_VegaFusionRuntime_innerPatchPreTransformedSpec<'local> (
    env: JNIEnv<'local>,
    class: JClass<'local>,
    spec1: JString<'local>,
    pre_transformed_spec1: JString<'local>,
    spec2: JString<'local>,
) -> jstring {
    let result = panic::catch_unwind(|| {
        inner_patch_pre_transformed_spec(env, class, spec1, pre_transformed_spec1, spec2)
    });

    match result {
        Ok(Ok(state)) => {
            state
        }
        Ok(Err(vf_err)) => {
            todo!("Raise VegaFusion Error")
        }
        Err(unwind_err) => {
            todo!("Raise Panic Error")
        }
    }
}

pub fn inner_patch_pre_transformed_spec<'local> (
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    spec1: JString<'local>,
    pre_transformed_spec1: JString<'local>,
    spec2: JString<'local>,
) -> Result<jstring> {
    // TODO no panics!
    let spec1: String = env
        .get_string(&spec1)
        .expect("Couldn't get java string!")
        .into();

    let pre_transformed_spec1: String = env
        .get_string(&pre_transformed_spec1)
        .expect("Couldn't get java string!")
        .into();

    let spec2: String = env
        .get_string(&spec2)
        .expect("Couldn't get java string!")
        .into();

    // Parse specs
    let spec1: ChartSpec = serde_json::from_str(&spec1)?;
    let pre_transformed_spec1: ChartSpec = serde_json::from_str(&pre_transformed_spec1)?;
    let spec2: ChartSpec = serde_json::from_str(&spec2)?;

    let pre_transformed_spec2 = patch_pre_transformed_spec(&spec1, &pre_transformed_spec1, &spec2)?;

    if let Some(pre_transformed_spec2) = pre_transformed_spec2 {
        let pre_transformed_spec2 = serde_json::to_string(&pre_transformed_spec2)?;

        // Then we have to create a new Java string to return. Again, more info
        // in the `strings` module.
        let output = env
            .new_string(pre_transformed_spec2)
            .expect("Couldn't create java string!"); // TODO: no panics

        // Finally, extract the raw pointer to return.
        Ok(output.into_raw())
    } else {
        // Return null
        todo!("Return null")
        // Ok(JObject::null().into_raw())
    }
}


#[no_mangle]
pub unsafe extern "system" fn Java_io_vegafusion_VegaFusionRuntime_innerPreTransformSpec<'local> (
    env: JNIEnv<'local>,
    class: JClass<'local>,
    pointer: jlong,
    spec: JString<'local>,
    local_tz: JString<'local>,
    default_input_tz: JString<'local>,
) -> jstring {
    let result = panic::catch_unwind(|| {
        inner_pre_transform_spec(env, class, pointer, spec, local_tz, default_input_tz)
    });

    match result {
        Ok(Ok(pre_transformed_spec)) => {
            pre_transformed_spec
        }
        Ok(Err(vf_err)) => {
            todo!("Raise VegaFusion Error")
        }
        Err(unwind_err) => {
            todo!("Raise Panic Error")
        }
    }
}

pub unsafe fn inner_pre_transform_spec<'local> (
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    pointer: jlong,
    spec: JString<'local>,
    local_tz: JString<'local>,
    default_input_tz: JString<'local>,
) -> Result<jstring> {
    let state = &mut *(pointer as *mut VegaFusionRuntimeState);
    let row_limit = None;
    let preserve_interactivity = true;
    // TODO no panics!
    let spec: String = env
        .get_string(&spec)
        .expect("Couldn't get java string!")
        .into();

    let local_tz: String = env
        .get_string(&local_tz)
        .expect("Couldn't get java string!")
        .into();

    let default_input_tz: String = env
        .get_string(&default_input_tz)
        .expect("Couldn't get java string!")
        .into();

    let spec: ChartSpec = serde_json::from_str(spec.as_str())?;

    let (pre_transformed_spec, warnings) = state.tokio_runtime.block_on(
        state.vf_runtime.pre_transform_spec(
            &spec,
            local_tz.as_str(),
            &Some(default_input_tz),
            row_limit,
            preserve_interactivity,
            Default::default(),
        )
    )?;
    let pre_transformed_spec = serde_json::to_string(&pre_transformed_spec)?;

    // Then we have to create a new Java string to return. Again, more info
    // in the `strings` module.
    let output = env
        .new_string(pre_transformed_spec)
        .expect("Couldn't create java string!"); // TODO: no panics

    // Finally, extract the raw pointer to return.
    Ok(output.into_raw())
}
