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
use std::panic;
use vegafusion_common::error::Result;
use vegafusion_core::patch::patch_pre_transformed_spec;
use vegafusion_core::spec::chart::ChartSpec;
use vegafusion_runtime::task_graph::runtime::VegaFusionRuntime;
use vegafusion_sql::connection::datafusion_conn::DataFusionConnection;
use vegafusion_sql::connection::Connection;

struct VegaFusionRuntimeState {
    pub vf_runtime: VegaFusionRuntime,
    pub tokio_runtime: tokio::runtime::Runtime,
}

#[no_mangle]
pub extern "system" fn Java_io_vegafusion_VegaFusionRuntime_version<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jstring {
    match inner_version(&env) {
        Ok(version) => version,
        Err(err) => {
            let _ = env.throw_new("io/vegafusion/VegaFusionException", err.to_string());
            JObject::null().into_raw()
        }
    }
}

fn inner_version(env: &JNIEnv) -> Result<jstring> {
    let version = env!("CARGO_PKG_VERSION");
    let output = env.new_string(version)?;
    Ok(output.into_raw())
}

#[no_mangle]
pub extern "system" fn Java_io_vegafusion_VegaFusionRuntime_innerCreate<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
) -> jlong {
    let result = panic::catch_unwind(|| inner_create());

    match result {
        Ok(Ok(state)) => Box::into_raw(Box::new(state)) as jlong,
        Ok(Err(vf_err)) => {
            let _ = env.throw_new("io/vegafusion/VegaFusionException", vf_err.to_string());
            return 0;
        }
        Err(_unwind_err) => {
            let _ = env.throw_new("io/vegafusion/VegaFusionException", "Uncaught Error");
            return 0;
        }
    }
}

fn inner_create() -> Result<VegaFusionRuntimeState> {
    // Use DataFusion connection and multi-threaded tokio runtime
    let conn = Arc::new(DataFusionConnection::default()) as Arc<dyn Connection>;
    let capacity = 64;
    let memory_limit = 2 << 30; // 2GB
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
pub unsafe extern "system" fn Java_io_vegafusion_VegaFusionRuntime_innerDestroy<'local>(
    _env: JNIEnv<'local>,
    _class: JClass<'local>,
    state_ptr: jlong,
) {
    // Cast/Box the state_ptr so that the drop logic will run
    let _boxed_state = Box::from_raw(state_ptr as *mut VegaFusionRuntimeState);
}

#[no_mangle]
pub unsafe extern "system" fn Java_io_vegafusion_VegaFusionRuntime_innerPatchPreTransformedSpec<
    'local,
>(
    mut env: JNIEnv<'local>,
    class: JClass<'local>,
    spec1: JString<'local>,
    pre_transformed_spec1: JString<'local>,
    spec2: JString<'local>,
) -> jstring {
    if let Ok((spec1, pre_transformed_spec1, spec2)) =
        parse_args_patch_pre_transformed_spec(&mut env, class, spec1, pre_transformed_spec1, spec2)
    {
        let result = panic::catch_unwind(|| {
            inner_patch_pre_transformed_spec(&spec1, &pre_transformed_spec1, &spec2)
        });

        match result {
            Ok(Ok(Some(patched_spec))) => match env.new_string(patched_spec) {
                Ok(patched_spec) => patched_spec.into_raw(),
                Err(err) => {
                    let _ = env.throw_new("io/vegafusion/VegaFusionException", err.to_string());
                    JObject::null().into_raw()
                }
            },
            Ok(Ok(None)) => {
                // Patch ran without error, but not patch result was found, return null
                JObject::null().into_raw()
            }
            Ok(Err(vf_err)) => {
                let _ = env.throw_new("io/vegafusion/VegaFusionException", vf_err.to_string());
                JObject::null().into_raw()
            }
            Err(_unwind_err) => {
                let _ = env.throw_new("io/vegafusion/VegaFusionException", "Uncaught Error");
                JObject::null().into_raw()
            }
        }
    } else {
        let _ = env.throw_new(
            "io/vegafusion/VegaFusionException",
            "Failed to parse args to innerPatchPreTransformedSpec",
        );
        JObject::null().into_raw()
    }
}

pub fn parse_args_patch_pre_transformed_spec<'local>(
    env: &mut JNIEnv<'local>,
    _class: JClass<'local>,
    spec1: JString<'local>,
    pre_transformed_spec1: JString<'local>,
    spec2: JString<'local>,
) -> Result<(String, String, String)> {
    let spec1: String = env.get_string(&spec1)?.into();

    let pre_transformed_spec1: String = env.get_string(&pre_transformed_spec1)?.into();

    let spec2: String = env.get_string(&spec2)?.into();

    Ok((spec1, pre_transformed_spec1, spec2))
}

pub fn inner_patch_pre_transformed_spec<'local>(
    spec1: &str,
    pre_transformed_spec1: &str,
    spec2: &str,
) -> Result<Option<String>> {
    // Parse specs
    let spec1: ChartSpec = serde_json::from_str(&spec1)?;
    let pre_transformed_spec1: ChartSpec = serde_json::from_str(&pre_transformed_spec1)?;
    let spec2: ChartSpec = serde_json::from_str(&spec2)?;

    let pre_transformed_spec2 = patch_pre_transformed_spec(&spec1, &pre_transformed_spec1, &spec2)?;

    if let Some(pre_transformed_spec2) = pre_transformed_spec2 {
        Ok(Some(serde_json::to_string(&pre_transformed_spec2)?))
    } else {
        // Return null
        Ok(None)
    }
}

struct PreTransformSpecArgs {
    spec: String,
    local_tz: String,
    default_input_tz: Option<String>,
    row_limit: Option<u32>,
    preserve_interactivity: bool,
}

fn parse_args_pre_transform_spec<'local>(
    env: &mut JNIEnv<'local>,
    spec: JString<'local>,
    local_tz: JString<'local>,
    default_input_tz: JString<'local>,
) -> Result<PreTransformSpecArgs> {
    let row_limit = None;
    let preserve_interactivity = true;
    let spec: String = env.get_string(&spec)?.into();
    let local_tz: String = env.get_string(&local_tz)?.into();
    let default_input_tz: Option<String> = Some(env.get_string(&default_input_tz)?.into());

    Ok(PreTransformSpecArgs {
        spec,
        local_tz,
        default_input_tz,
        row_limit,
        preserve_interactivity,
    })
}

unsafe fn inner_pre_transform_spec<'local>(
    pointer: jlong,
    args: PreTransformSpecArgs,
) -> Result<String> {
    let state = &mut *(pointer as *mut VegaFusionRuntimeState);
    let spec: ChartSpec = serde_json::from_str(args.spec.as_str())?;

    // TODO: Handle warnings (Stash in spec metadata?)
    let (pre_transformed_spec, _warnings) =
        state
            .tokio_runtime
            .block_on(state.vf_runtime.pre_transform_spec(
                &spec,
                args.local_tz.as_str(),
                &args.default_input_tz,
                args.row_limit,
                args.preserve_interactivity,
                Default::default(),
            ))?;
    let pre_transformed_spec = serde_json::to_string(&pre_transformed_spec)?;
    Ok(pre_transformed_spec)
}

#[no_mangle]
pub unsafe extern "system" fn Java_io_vegafusion_VegaFusionRuntime_innerPreTransformSpec<'local>(
    mut env: JNIEnv<'local>,
    _class: JClass<'local>,
    pointer: jlong,
    spec: JString<'local>,
    local_tz: JString<'local>,
    default_input_tz: JString<'local>,
) -> jstring {
    if let Ok(args) = parse_args_pre_transform_spec(&mut env, spec, local_tz, default_input_tz) {
        let result = panic::catch_unwind(|| inner_pre_transform_spec(pointer, args));

        match result {
            Ok(Ok(pre_transformed_spec)) => match env.new_string(pre_transformed_spec) {
                Ok(pre_transformed_spec) => pre_transformed_spec.into_raw(),
                Err(err) => {
                    let _ = env.throw_new("io/vegafusion/VegaFusionException", err.to_string());
                    JObject::null().into_raw()
                }
            },
            Ok(Err(vf_err)) => {
                let _ = env.throw_new("io/vegafusion/VegaFusionException", vf_err.to_string());
                JObject::null().into_raw()
            }
            Err(_unwind_err) => {
                let _ = env.throw_new("io/vegafusion/VegaFusionException", "Uncaught Error");
                JObject::null().into_raw()
            }
        }
    } else {
        let _ = env.throw_new(
            "io/vegafusion/VegaFusionException",
            "Failed to parse args to innerPreTransformSpec",
        );
        JObject::null().into_raw()
    }
}

//
// #[no_mangle]
// pub unsafe extern "system" fn Java_io_vegafusion_VegaFusionRuntime_innerPreTransformSpec<'local> (
//     mut env: JNIEnv<'local>,
//     class: JClass<'local>,
//     pointer: jlong,
//     spec: JString<'local>,
//     local_tz: JString<'local>,
//     default_input_tz: JString<'local>,
// ) -> jstring {
//     let result = panic::catch_unwind(|| {
//         // inner_pre_transform_spec(&mut env, class, pointer, spec, local_tz, default_input_tz)
//     });
//
//     match result {
//         Ok(Ok(pre_transformed_spec)) => {
//             pre_transformed_spec
//         }
//         Ok(Err(vf_err)) => {
//             let _ = env.throw_new("io/vegafusion/VegaFusionException", vf_err.to_string());
//             JObject::null().into_raw()
//         }
//         Err(_unwind_err) => {
//             let _ = env.throw_new("io/vegafusion/VegaFusionException", "Uncaught Error");
//             JObject::null().into_raw()
//         }
//     }
// }
//
// pub unsafe fn inner_pre_transform_spec<'local> (
//     env: &mut JNIEnv<'local>,
//     _class: JClass<'local>,
//     pointer: jlong,
//     spec: JString<'local>,
//     local_tz: JString<'local>,
//     default_input_tz: JString<'local>,
// ) -> Result<jstring> {
//     let state = &mut *(pointer as *mut VegaFusionRuntimeState);
//     let row_limit = None;
//     let preserve_interactivity = true;
//     let spec: String = env.get_string(&spec)?.into();
//     let local_tz: String = env.get_string(&local_tz)?.into();
//     let default_input_tz: String = env.get_string(&default_input_tz)?.into();
//
//     let spec: ChartSpec = serde_json::from_str(spec.as_str())?;
//
//     let (pre_transformed_spec, warnings) = state.tokio_runtime.block_on(
//         state.vf_runtime.pre_transform_spec(
//             &spec,
//             local_tz.as_str(),
//             &Some(default_input_tz),
//             row_limit,
//             preserve_interactivity,
//             Default::default(),
//         )
//     )?;
//     let pre_transformed_spec = serde_json::to_string(&pre_transformed_spec)?;
//
//     // Then we have to create a new Java string to return. Again, more info
//     // in the `strings` module.
//     let output = env.new_string(pre_transformed_spec)?;
//
//     // Finally, extract the raw pointer to return.
//     Ok(output.into_raw())
// }
